package paxos

import "net"
import "net/rpc"
import "log"
import "syscall"
import "fmt"

// return true if p1 < p2
func lt(p1 PropNum, p2 PropNum) bool {
	return p1.Num < p2.Num || (p1.Num == p2.Num && p1.Srv < p2.Srv)
}

func (px *Paxos) newPropNum() PropNum {
	// log.Println("newPropNum srv =", px.me, " time = ", px.time)
	var pn PropNum
	pn.Srv = px.me
	pn.Num = px.time
	px.time = px.time+1
	return pn
}

func (px *Paxos) prepare(i int, seq int, n PropNum, done int) (bool, PropNum, interface{}, int, bool) {
	var args PrepareArgs
	var reply PrepareReply
	args.Seq = seq
	args.N = n
	args.Done = done
	args.Peer = px.me
	ok := true
	if i == px.me {
		px.Prepare(&args, &reply)
	} else {
		ok = call(px.peers[i], "Paxos.Prepare", args, &reply)
	}
	return reply.Status == PrepareOK, reply.N, reply.V, reply.Hpn, ok
}

func (px *Paxos) accept(i int, seq int, n PropNum, v interface{}) (bool, PropNum, int, bool) {
	var args AcceptArgs
	var reply AcceptReply
	args.Seq = seq
	args.N = n
	args.V = v
	ok := true
	if i == px.me {
		px.Accept(&args, &reply)
	} else {
		ok = call(px.peers[i], "Paxos.Accept", args, &reply)
	}
	return reply.Status == AcceptOK, reply.N, reply.Hpn, ok
}

func (px *Paxos) decide(i int, seq int, v interface{}) bool {
	var args DecideArgs
	var reply DecideReply
	args.Seq = seq
	args.V = v
	ok := true
	if i == px.me {
		px.Decide(&args, &reply)
	} else {
		ok = call(px.peers[i], "Paxos.Decide", args, &reply)
	}
	return ok
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}
	log.Println("RPC call return error: ", err)
	return false
}
