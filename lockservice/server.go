package lockservice

import "net"
import "net/rpc"
import "sync"
import "log"
import "os"
import "io"
import "time"

func assert(x bool) {
	if !x {
		log.Println("WARNING")
	}
}

type LockServer struct {
	mu    sync.Mutex
	l     net.Listener
	dead  bool // for test_test.go
	dying bool // for test_test.go

	am_primary bool   // am I the primary?
	backup     string // backup's port

	// for each lock name, is it locked?
	locks map[string]bool

	// transactions saved
	saved map[int64]bool

	backupDead bool
}

func tryToLock(lock string, table map[string]bool) bool {
	locked, status := table[lock]
	if locked && status {
		//log.Println("can't lock", lock)
		return false
	} else {
		//log.Println("just locked", lock)
		table[lock] = true
		return true
	}
}

func tryToUnlock(lock string, table map[string]bool) bool {
	locked, status := table[lock]
	table[lock] = false
	if locked && status {
		//log.Println("lock had been held", lock)
		return true
	} else {
		//log.Println("just unlocked", lock)
		return false
	}
}

func (ls *LockServer) lockBackup(args *LockArgs, reply *LockReply) {
	assert(ls.am_primary == false)
	s, b := ls.saved[args.TransNum]
	if b {
		log.Println("Backup: returning saved", s, "for", args.TransNum)
		reply.OK = s
		return
	}
	reply.OK = tryToLock(args.Lockname, ls.locks)
	log.Println("Backup: saving trans num", args.TransNum, "with", reply.OK)
	ls.saved[args.TransNum] = reply.OK
	return
}

func (ls *LockServer) unlockBackup(args *UnlockArgs, reply *UnlockReply) {
	assert(ls.am_primary == false)
	s, b := ls.saved[args.TransNum]
	if b {
		log.Println("Backup: return saved", s)
		reply.OK = s
		return
	}
	reply.OK = tryToUnlock(args.Lockname, ls.locks)
	log.Println("Backup: saving trans num", args.TransNum, "with", reply.OK)
	ls.saved[args.TransNum] = reply.OK
	return
}

//
// server Lock RPC handler.
//
// you will have to modify this function
//
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.am_primary {
		log.Println("Primary: receive locking request", args.Lockname)
	} else {
		log.Println("Backup: receiving locking request ", args.Lockname)
		log.Println(ls.saved)
	}

	if ls.am_primary == false {
		ls.lockBackup(args, reply)
	} else {
		var backup_reply LockReply
		ok := call(ls.backup, "LockServer.Lock", args, &backup_reply)
		if ok == false {
			reply.OK = tryToLock(args.Lockname, ls.locks)
		} else {
			reply.OK = backup_reply.OK
			if reply.OK {
				//log.Println("locking ", args.Lockname, " on primary")
				assert(ls.locks[args.Lockname] == false)
				ls.locks[args.Lockname] = true
			} else {
				assert(ls.locks[args.Lockname] == true)
			}
		}
	}
	return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.am_primary {
		log.Println("Primary: receive unlocking request", args.Lockname)
	} else {
		log.Println("Backup: receiving unlocking request ", args.Lockname)
		log.Println(ls.saved)
	}

	//log.Println("requesting unlock on ", args.Lockname, "to", ls.am_primary)

	if !ls.am_primary {
		ls.unlockBackup(args, reply)
		return nil
	} else {
		var backup_reply LockReply
		ok := call(ls.backup, "LockServer.Unlock", args, &backup_reply)
		if ok == false {
			reply.OK = tryToUnlock(args.Lockname, ls.locks)
		} else {
			reply.OK = backup_reply.OK
			if reply.OK {
				//log.Println("unlocking ", args.Lockname, " on primary")
				assert(ls.locks[args.Lockname] == true)
				ls.locks[args.Lockname] = false
			} else {
				assert(ls.locks[args.Lockname] == false)
			}
		}

	}

	return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
	ls.dead = true
	ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
	c io.ReadWriteCloser
}

func (dc DeafConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}
func (dc DeafConn) Close() error {
	return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
	return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
	log.Println("Starting server")
	ls := new(LockServer)
	ls.backup = backup
	ls.am_primary = am_primary
	ls.locks = map[string]bool{}
	ls.saved = map[int64]bool{}
	ls.backupDead = false

	// Your initialization code here.
	// log.Println("server started with name ", primary, backup)

	me := ""
	if am_primary {
		me = primary
	} else {
		me = backup
	}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(ls)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ls.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for ls.dead == false {
			conn, err := ls.l.Accept()
			if err == nil && ls.dead == false {
				if ls.dying {
					// process the request but force discard of reply.

					// without this the connection is never closed,
					// b/c ServeConn() is waiting for more requests.
					// test_test.go depends on this two seconds.
					go func() {
						time.Sleep(2 * time.Second)
						conn.Close()
					}()
					ls.l.Close()

					// this object has the type ServeConn expects,
					// but discards writes (i.e. discards the RPC reply).
					deaf_conn := DeafConn{c: conn}

					rpcs.ServeConn(deaf_conn)

					ls.dead = true
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && ls.dead == false {
				log.Printf("LockServer(%v) accept: %v\n", me, err.Error())
				ls.kill()
			}
		}
	}()

	return ls
}
