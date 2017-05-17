package paxos

// Structures for RPC communications

type PropNum struct {
	Num int
	Srv int
}

type DecideArgs struct {
	Seq int
	V   interface{}
}

type DecideReply struct {
}

type PrepareArgs struct {
	Seq int
	N   PropNum
    Done   int
    Peer   int
}

const PrepareOK = "prepare_ok"
const PrepareReject = "prepare_reject"

type PrepareReply struct {
	Status string
	N      PropNum
	V      interface{}
    Hpn    int
}

type AcceptArgs struct {
	Seq int
	N   PropNum
	V   interface{}
}

const AcceptOK = "accept_ok"
const AcceptReject = "accept_reject"

type AcceptReply struct {
	Status string
	N      PropNum
    Hpn    int
}

