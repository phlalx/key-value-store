package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// proposer state
	// needed to generate new propNum
	time int
	// local done
	done int

	// per Instance acceptor state
	acceptor  map[int]Acceptor
	knownMins []int
	lastFree  int // last known min
}

type Acceptor struct {
	// acceptor state
	// highest prepare seen
	n_p PropNum
	// highest accept seen
	n_a PropNum
	// corresponding value
	v_a interface{}
	// decided value
	dv interface{}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()

	// log.Println("Server", px.me, ": Start ", seq, ", proposed val =", v)
	if seq < px.min() {
		return
	}
	px.mu.Unlock()
	go func() {
		px.proposer(seq, v)
	}()
}

func (px *Paxos) tryToFreeMem() {
	if px.min() != px.lastFree {
		// time to Free memory
		// we can remove anything < px.min() (and >= px.lastFree)
		for k, _ := range px.acceptor {
			if k < px.min() {
				delete(px.acceptor, k)
			}
		}
	}
}

// rpc method must return something?
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	n := args.N
	//log.Println("Server", px.me, ": Prepare seq =", seq, ", prop =", n)
	inst, ok := px.acceptor[seq]
	if !ok {
		var inst Acceptor
		unknown := PropNum{-1, -1}
		inst.n_a = unknown
		inst.n_p = n
		inst.v_a = nil
		inst.dv = nil
		px.acceptor[seq] = inst
		reply.N = inst.n_a
		reply.V = inst.v_a
		reply.Status = PrepareOK
	} else if lt(inst.n_p, n) {
		inst.n_p = n
		px.acceptor[seq] = inst
		reply.N = inst.n_a
		reply.V = inst.v_a
		reply.Status = PrepareOK
	} else {
		reply.Status = PrepareReject
		reply.Hpn = inst.n_p.Num // TODO
	}
	//	log.Println("Server", px.me, "knownMins = ", px.knownMins)
	if px.knownMins[args.Peer] != args.Done {
		px.knownMins[args.Peer] = args.Done
		px.tryToFreeMem()
	}
	//	log.Println("Server", px.me, "knownMins = ", px.knownMins)
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	n := args.N
	v := args.V
	// log.Println("Server", px.me, ": Accept ", seq, ",", n, ", val =", v)
	inst := px.acceptor[seq]
	if lt(inst.n_p, n) || inst.n_p == n {
		inst.n_p = n
		inst.n_a = n
		inst.v_a = v
		px.acceptor[seq] = inst
		reply.Status = AcceptOK
		reply.N = n
	} else {
		reply.Status = AcceptReject
		reply.Hpn = inst.n_p.Num // TODO
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	v := args.V
	// log.Println("Server", px.me, ": Decide ", seq, ",", v)
	inst := px.acceptor[seq]
	inst.dv = v
	px.acceptor[seq] = inst
	return nil
}

func (px *Paxos) proposer(seq int, value interface{}) {
	//log.Println("Proposer ", px.me, ": proposer seq =", seq, ", value = ", value)
	decided := false
	majority := len(px.peers)/2 + 1
	for !decided {
		px.mu.Lock()
		n := px.newPropNum()
		px.mu.Unlock()
		unknown := PropNum{-1, -1}
		maxNum := unknown
		var maxVal interface{}
		maxVal = nil
		answers := 0

		//log.Println("Proposer ", px.me, ": broadcasts PREPARE seq =", seq, ", n =", n)
		for i := range px.peers {
			st, n_a, v_a, hpn, ok := px.prepare(i, seq, n, px.done)
			if !ok {
				continue
			}
			if !st {
                if px.time < hpn {
                    px.time = hpn
                }
				continue
			}
			answers++
			if lt(maxNum, n_a) {
				maxNum = n_a
				maxVal = v_a
			}
		}
		if answers < majority {
			continue
		}

		var vprime interface{}
		if maxVal == nil {
			vprime = value
		} else {
			vprime = maxVal
		}
		//log.Println("Proposer", px.me, " broadcasts ACCEPT seq = ", seq, "n =", n, "vprime = ", vprime)
		count := 0
		for i := range px.peers {
			st, nres, hpn, ok := px.accept(i, seq, n, vprime)
			if st && (nres == n) && ok {
				count++
			}
            if !st {
                if px.time < hpn {
                    px.time = hpn
                }
            }
		}
		if count >= majority {
			//log.Println("Proposer", px.me, " has decided. Majority out of", count, "answers")
			for i := range px.peers {
				px.decide(i, seq, vprime)
			}

			decided = true
		}

	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.done = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	m := 0
	for k, _ := range px.acceptor {
		if k > m {
			m = k
		}
	}
	return m
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest Number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.min()
	//log.Println("Server", px.me, ": Min", min)
	return min
}

func (px *Paxos) min() int {
	min := px.knownMins[0]
	for i := range px.knownMins {
		if px.knownMins[i] < min {
			min = px.knownMins[i]
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	//log.Println("Server", px.me, ": Status ", seq)
	inst := px.acceptor[seq]
	dv := inst.dv
	if seq < px.min() {
		//log.Println("Server", px.me, ": Status. This instance has been forgotten ", seq)
		return false, dv
	}

	if dv != nil {
		//log.Println("Server", px.me, ": has decided value ?" /* dv,*/, "for seq", seq)
	}
	return dv != nil, dv
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.done = -1
	px.knownMins = make([]int, len(peers))
	for i := range px.knownMins {
		px.knownMins[i] = -1
	}
	px.lastFree = px.min() // 0

	px.acceptor = make(map[int]Acceptor)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
