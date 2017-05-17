package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	view            View
	lastKnown       map[string]time.Time
	primaryValidate bool
	init            bool
}

func (vs *ViewServer) changeView(v View) {
	v.Viewnum++
	vs.view = v
	vs.primaryValidate = false
	log.Println("VS: changing view", vs.ToString())
}

func (vs *ViewServer) bDead() bool {
	// precondition : primary != ""
	// precondition : lastKnown[primary] != nil
	span := time.Now().Sub(vs.lastKnown[vs.view.Backup])
	return span > 5*PingInterval
}

func (vs *ViewServer) pDead() bool {
	// precondition : primary != ""
	// precondition : lastKnown[primary] != nil
	span := time.Now().Sub(vs.lastKnown[vs.view.Primary])
	return span > 5*PingInterval
}

func (vs *ViewServer) ToString() string {
	v := vs.view
	s := " P = " + ServerNameToString(v.Primary) + " B = " +
		ServerNameToString(v.Backup)
	return s
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	v := vs.view
	//log.Println("VS: ping from ", serverToString(args.Me), " with view ",
	//	args.Viewnum, "server view = ", vs.ToString(), "#", v.Viewnum, "PP")
	vs.lastKnown[args.Me] = time.Now()

	if !vs.init {
		// setup Primary
		v.Primary = args.Me
		vs.init = true
		vs.changeView(v)
	} else if v.Backup == "" && args.Me != v.Primary && vs.primaryValidate {
		// setup Backup
		v.Backup = args.Me
		vs.changeView(v)
	} else if args.Me == v.Primary && v.Viewnum == args.Viewnum {
		vs.primaryValidate = true
	} else if args.Viewnum == 0 && args.Me == v.Primary && vs.primaryValidate {
		v.Primary = v.Backup
		v.Backup = ""
		vs.changeView(v)
	} else if args.Viewnum == 0 && args.Me == v.Backup && vs.primaryValidate {
		v.Backup = ""
		vs.changeView(v)
	}
	reply.View = v

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	log.Println("Viewserver : Get ",  vs.ToString())
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.view.Primary != "" && vs.pDead() && vs.primaryValidate {
		log.Print("VS: P is dead")
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		vs.primaryValidate = false
		vs.view.Viewnum++
		log.Println("VS: changing view", vs.ToString())
	}
	if vs.view.Backup != "" && vs.bDead() && vs.primaryValidate {
		log.Print("VS: B is dead")
		vs.view.Backup = ""
		vs.view.Viewnum++
		vs.primaryValidate = false
		log.Println("VS: changing view", vs.ToString())
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.lastKnown = make(map[string]time.Time)
	// Your vs.* initializations here.
	v := View{Viewnum: 0}
	vs.view = v

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
