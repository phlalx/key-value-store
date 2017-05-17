package kvpaxos

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

const GET = 0
const PUT = 1

type Op struct {
    Id   int64     // server address, useful to identify server proposition from older values in log
	Op    int
	Key   string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	log []Op
}

func waitStatus(kv *KVPaxos, seq int) Op {
    for {
		to := 10 * time.Millisecond
		for {
			decided, value := kv.px.Status(seq)
			if decided {
                return value.(Op)
			}
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
    }
}

func (kv *KVPaxos) findValue(key string) (string, bool) {
    log.Println("Server", kv.me, "lookup key", key)
    var value string
    ok := false
    for i := len(kv.log) - 1; i >= 0; i-- {
        if kv.log[i].Op == PUT && kv.log[i].Key == key {
            value = kv.log[i].Value
            ok = true
            break;
        }
    }
    return value, ok
}

func (kv *KVPaxos) checkLog() {
    check := make(map[int64]bool)
    for _, op := range kv.log {
        if check[op.Id] == true {
            fmt.Println("---BUG---")
        }
        check[op.Id] = true
    }
}


func (kv *KVPaxos) logToString() {
    for i, op := range kv.log {
        if op.Op == PUT {
            log.Println("serv = ", kv.me, "seq =", i, "op = PUT key =", op.Key,
            "value =  op.Value")
        } else {
            log.Println("serv = ", kv.me, "seq =", i, "op = GET key =", op.Key)
        }
    }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
    log.Println("Server", kv.me, "Get ", args.Key)
	kv.mu.Lock()   // TODO try finer lock granularity
	defer kv.mu.Unlock()
    kv.checkLog()
    op := Op{}
    op.Op = GET
    op.Key = args.Key
    op.Id = args.Id

    done := false
    var seq int
    for !done {
        seq = len(kv.log)
        log.Println("Server", kv.me, "calls Starts(Get) with seq = ", seq, " key = ", args.Key)
        kv.px.Start(seq, op)
        res := waitStatus(kv, seq)
        kv.log = append(kv.log, res)
        if res == op  {
            var ok bool
            reply.Value, ok = kv.findValue(op.Key) 
            if ok {
                reply.Err = OK
            } else {
                reply.Err = ErrNoKey
            }
            done = true
        }
    }
    kv.px.Done(seq)
    // kv.logToString()
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
    log.Println("Server", kv.me, "Put ", args.Key, "Val")//,  args.Value)
	kv.mu.Lock()   // TODO try finer lock granularity
	defer kv.mu.Unlock()

    op := Op{}
    op.Op = PUT 
    op.Key = args.Key
    op.Value = args.Value
    op.Id = args.Id

    done := false
    var seq int
    for !done {
        seq = len(kv.log)
        log.Println("Server", kv.me, "calls Starts(Put) with seq = ", seq, " key = ", args.Key)
        kv.px.Start(seq, op)
        res := waitStatus(kv, seq)
        kv.log = append(kv.log, res)
        if res == op {
            reply.Err = OK
            done = true
        } 
    }
    kv.px.Done(seq)
    // kv.logToString() 
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// this call is all that's needed to persuade
	// Go's RPC library to marshall/unmarshall
	// struct Op.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	// kv.log = make([]Op,0) // not needed

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
