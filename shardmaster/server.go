package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "pods/paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	pxid    int      // next sequence number to propose
}

const Join = 0
const Leave = 1
const Move = 2
const Query = 3

type Op struct {
	Num     int
	Op      int
	Gid     int64
	Servers []string
	Shard   int
	Id      int64
}

func contains(servers []string, srv string) bool {
	for _, s := range servers {
		if s == srv {
			return true
		}
	}
	return false
}

func min(freq map[int64]int) int64 {
	kmin, vmin := int64(0), NShards
	for k, v := range freq {
		if v <= vmin {
			kmin = k
			vmin = v
		}
	}
	freq[kmin]++
	return kmin
}

func (sm *ShardMaster) add(op Op, seq int) Config {
	// update sm.configs with operation op
	//alert := ""
	//if seq != sm.pxid {
	//    alert = "ALERT"
	//}
	//log.Print("Shardmaster ", sm.me, " add op(",op.Op,") id =",op.Id % 1000, " seq = ", seq, " sm.pxid = ", sm.pxid, alert)
	sm.pxid++
	n := len(sm.configs)
	c_last := sm.configs[n-1]
	if op.Op == Query {
		return c_last
	}
	var shards [NShards]int64
	groups := make(map[int64][]string)
	for i := range c_last.Shards {
		shards[i] = c_last.Shards[i]
	}
	for k, v := range c_last.Groups {
		tmp := make([]string, len(v))
		copy(tmp, v)
		groups[k] = tmp
	}
	num := c_last.Num + 1
	c := Config{num, shards, groups}
	switch op.Op {
	case Join:
		// log.Print("before ", c.toString())
		for _, s := range op.Servers {
			if !contains(c.Groups[op.Gid], s) {
				c.Groups[op.Gid] = append(c.Groups[op.Gid], s)
			}
		}
		i := 0
		for i < NShards {
			c.Shards[i] = op.Gid
			i += len(c.Groups)
		}
		// log.Print("after ", c.toString())
	case Leave:
		delete(c.Groups, op.Gid)
		i := 0
		freq := make(map[int64]int)
		//log.Print("c.Shards", c.Shards)
		for s := range c.Groups {
			freq[s] = 0
			for i := range c.Shards {
				if c.Shards[i] == s {
					freq[s]++
				}
			}
		}

		l := make([]int64, 0)
		for k, _ := range c.Groups {
			l = append(l, k)
		}
		for i < NShards {
			if c.Shards[i] == op.Gid {
				c.Shards[i] = min(freq)
			}
			i++
		}
	case Move:
		c.Shards[op.Shard] = op.Gid
	case Query:
	}
	sm.configs = append(sm.configs, c)
	log.Print("Shardmaster ", sm.me, " configs[", len(sm.configs)-1,"] = ", c.toString())
	return c
}

func (sm *ShardMaster) waitStatus(seq int) Op {

	to := 10 * time.Millisecond
	for {
		// log.Print("Shardmaster ", sm.me, " waiting seq", seq)
		decided, value := sm.px.Status(seq)
		if decided {
			// log.Print("Shardmaster ", sm.me, " decided", seq)
			return value.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

}

func (sm *ShardMaster) proposeOp(op Op) int {
	done := false
	op.Id = rand.Int63()
	var seq int
	for !done {
		// log.Println("ShardMaster", sm.me, "not done")
		seq = sm.pxid
		//log.Println("ShardMaster", sm.me, "propose seq = ", seq, " op.Id =", op.Id % 1000)
		sm.px.Start(seq, op)
		res := sm.waitStatus(seq)
		done = (res.Id == op.Id)
		if done {
			//log.Println("ShardMaster", sm.me, "proposition  op.Id =", op.Id % 1000, "accepted with seq = ", seq)
			break
		}
		//log.Println("ShardMaster", sm.me, "proposition  op.Id =", op.Id % 1000, "refused. Instead res.Id = ", res.Id % 1000, "with seq = ", seq)
		sm.add(res, seq)
	}
	sm.px.Done(sm.pxid)
	return seq
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Println("ShardMaster", sm.me, "Join GID", args.GID)
	gid := args.GID
	servers := args.Servers
	op := Op{Op: Join, Gid: gid, Servers: servers}
	seq := sm.proposeOp(op)
	sm.add(op, seq)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Println("ShardMaster", sm.me, "Leave GID", args.GID)
	gid := args.GID
	op := Op{Op: Leave, Gid: gid}
	seq := sm.proposeOp(op)
	sm.add(op, seq)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	log.Println("ShardMaster", sm.me, "Move GID", args.GID)
	gid := args.GID
	op := Op{Op: Move, Gid: gid, Shard: args.Shard}
	seq := sm.proposeOp(op)
	sm.add(op, seq)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// log.Println("ShardMaster", sm.me, "Query", args.Num)
	op := Op{Op: Query, Num: args.Num}
	seq := sm.proposeOp(op)
	conf := sm.add(op, seq)
	if args.Num == -1 {
		reply.Config = conf
	} else {
		reply.Config = sm.configs[args.Num]
	}
	//log.Println("ShardMaster", sm.me, "Query Reply config ", reply.Config.Num)
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
