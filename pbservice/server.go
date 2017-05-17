package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "pods/viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	init       bool // true after initialization
	me         string
	vs         *viewservice.Clerk
	view       viewservice.View
	table      map[string]string
}

func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Print("Server ", viewservice.ServerNameToString(pb.me), ": GET ", args.Key,
		" isPrimary:", pb.isPrimary())

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	k := args.Key
	a, ok := pb.table[k]
	if ok {
		reply.Value = a
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	log.Print("Server ", viewservice.ServerNameToString(pb.me), ": GET answer",
    reply.Value)
	log.Print("Server map: ", pb.table)


	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	log.Print("Server ", viewservice.ServerNameToString(pb.me), ": PUT ",
		args.Key, " ", args.Value)

	if pb.isPrimary() && pb.view.Backup != "" {
		log.Print("Primary ", viewservice.ServerNameToString(pb.view.Primary),
			" -> Backup ", viewservice.ServerNameToString(pb.view.Backup),
			": forwarding Put "+args.Key, " ", args.Value)
		forward_args := args
		var forward_reply PutReply
		ok := call(pb.view.Backup, "PBServer.Put", forward_args, &forward_reply)
		if !ok {
			log.Print("Server : Primary can't forward op to backup")
            reply.Err = "Can't connect to backup"
            return nil
		}
	}

	reply.Err = OK
	//	if !pb.isPrimary() {
	//			reply.Err = ErrWrongServer
	//	}

	k := args.Key
	v := args.Value
	pb.table[k] = v

	return nil
}

func (pb *PBServer) Init(args *InitArgs, reply *InitReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	log.Print("Server ", viewservice.ServerNameToString(pb.me), ": INIT ")
	pb.table = args.Table
	pb.init = true
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	vn := pb.view.Viewnum
	vs := pb.vs
	v, _ := vs.Ping(vn)
	changed := false
	if pb.view.Backup == "" && v.Backup != "" && pb.view.Primary == pb.me {
		changed = true
	}
	if v.Viewnum != pb.view.Viewnum {
		pb.view = v
	}
	if changed {
		log.Println("Primary -> Backup : sending init")
		var init_args InitArgs
		init_args.Table = pb.table
		var init_reply InitReply
		ok := call(v.Backup, "PBServer.Init", &init_args, &init_reply)
		if !ok {
			log.Print("Problem with init")
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.init = false
	pb.vs = viewservice.MakeClerk(me, vshost)
    // log.Print("Start server", viewservice.ServerNameToString(me))
	pb.table = make(map[string]string)
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
