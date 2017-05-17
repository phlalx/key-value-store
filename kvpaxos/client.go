package kvpaxos


import "net/rpc"
import "time"
import "crypto/rand"
import "math/big"

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &GetArgs{}
			args.Key = key
            args.Id = nrand()
			var reply GetReply
			ok := call(srv, "KVPaxos.Get", args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	// You will have to modify this function.

	for {
		for _, srv := range ck.servers {
			args := &PutArgs{}
			args.Key = key
			args.Value = value
            args.Id = nrand()
			var reply PutReply
			ok := call(srv, "KVPaxos.Put", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
