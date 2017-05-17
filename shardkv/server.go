package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "pods/paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "pods/shardmaster"
import "strconv"

const GET = 0
const PUT = 1
const RECONF = 2
const GETSHARD = 3

type Op struct {
	Id    int64
	Op    int
	Key   string
	Value string
	Conf  shardmaster.Config // new config for Reconf
	N     int                // Conf number for GetShard
}

func (op Op) toString() string {
	var res string
	switch op.Op {
	case GET:
		res = "GET " + op.Key
	case PUT:
		res = "PUT " + op.Key + "=" + op.Value
	case RECONF:
		res = "RECONF " + strconv.Itoa(op.Conf.Num)
	case GETSHARD:
		res = "GETSHARD " + strconv.Itoa(op.N)
	}
	return res
}

func (kv *ShardKV) printState() {
	for i, op := range kv.log {
		log.Print(i, " ", op.toString)
	}
}

type table map[string]string

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	conf  shardmaster.Config
	kvmap table
	log   []Op

	savepoints   map[int]table
	updateNeeded bool
	oldConf      shardmaster.Config
}

func (kv *ShardKV) waitStatus(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided {
			return value.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// depending on the operation to be performed, several return values are
// possible
// Common : ok == false => can't perform until update is done
// PUT:
// GET: val, isPres
// GETSHARD: kvmap, isKnown
// RECONF:
type paxos_return struct {
	val       string
	isInTable bool
	kvmap     table
	isKnown   bool
	ok        bool
}

func (kv *ShardKV) performPaxosOp(op Op) paxos_return {
	var val string
	var ok bool
	done := false
	var seq int

	retval := paxos_return{}
	// retval.kvmap = make(table)
	retval.ok = true

	log.Println("kv", kv.me, "(", kv.gid, ") perform paxos")
	if kv.updateNeeded && (op.Op != GETSHARD || (op.Op == GETSHARD && op.N >= kv.conf.Num)) {
		log.Println("kv", kv.me, "(", kv.gid, ") can't perfom op because update is needed")
        retval.ok = false
        return retval
	}

	for !done {
		seq = len(kv.log)
		kv.px.Start(seq, op)
		res := kv.waitStatus(seq)
		kv.log = append(kv.log, res)
		catchup := "(catch up)"
		if res.Op == op.Op && res.Id == op.Id {
			done = true
			catchup = ""
		}
		log.Println("kv", kv.me, "(", kv.gid, ") log[", len(kv.log)-1, "]", res.toString(), catchup)
		// apply op to server state
		switch res.Op {
		case PUT:
			kv.kvmap[res.Key] = res.Value
			log.Println("kv", kv.me, "(", kv.gid, ") PUT kv[", res.Key, "]=", res.Value)
		case GET:
			retval.val, retval.isInTable = kv.kvmap[res.Key]
			if ok {
				log.Println("kv", kv.me, "(", kv.gid, ") GET kv[", res.Key, "] ->", val)
			} else {
				log.Println("kv", kv.me, "(", kv.gid, ") GET kv[", res.Key, "] -> err")
			}
		case GETSHARD:
			log.Println("kv", kv.me, "(", kv.gid, ") GETSHARD =", res.N)
			retval.kvmap, retval.isKnown = kv.savepoints[res.N]
		case RECONF:
			log.Println("kv", kv.me, "(", kv.gid, ") RECONF num =", res.Conf.Num)
			kv.oldConf = kv.conf
			kv.conf = res.Conf
			if kv.oldConf.Num+1 != kv.conf.Num {
				panic("problem with reconf")
			}

			if kv.oldConf.Num == 0 { // Nothing todo when updating for the first
				// time
				break
			}

			tmp := make(table)
			for k, v := range kv.kvmap {
				tmp[k] = v
			}
			log.Println("kv", kv.me, "(", kv.gid, ") save kvmap at conf num =", kv.oldConf.Num, "map =", tmp)
			kv.savepoints[kv.oldConf.Num] = tmp
			log.Println("kv", kv.me, "(", kv.gid, ") needs shard updating")
			kv.updateNeeded = true
		default:
			panic("invalid op code")
		}
	}
	kv.px.Done(seq)
	return retval
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	log.Println("kv", kv.me, "(", kv.gid, ") Get (waitlock)", args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Println("kv", kv.me, "(", kv.gid, ") Get", args.Key)
	op := Op{}
	op.Op = GET
	op.Key = args.Key
	op.Id = args.Id

	if !kv.checkKey(op.Key) {
		reply.Err = ErrWrongGroup
		return nil
	}

	retval := kv.performPaxosOp(op)
	if !retval.ok {
		reply.Err = ErrWaitingUpdate
		return nil
	}
	if retval.isInTable {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	reply.Value = retval.val
	// kv.printState()
	return nil
}

func (kv *ShardKV) checkKey(k string) bool {
	var res bool
	if kv.conf.Num == 0 {
		res = false
	} else {
		res = kv.conf.Shards[key2shard(k)] == kv.gid
	}
	return res
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	log.Println("kv", kv.me, "(", kv.gid, ") Put (waitlock)", args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Println("kv", kv.me, "(", kv.gid, ") Put (", args.Key, ",", args.Value, ")")
	op := Op{}
	op.Op = PUT
	op.Key = args.Key
	op.Value = args.Value
	op.Id = args.Id
	if !kv.checkKey(op.Key) {
		reply.Err = ErrWrongGroup
		return nil
	}
	retval := kv.performPaxosOp(op)
	if retval.ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWaitingUpdate
	}
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	log.Println("kv", kv.me, "(", kv.gid, ") GetShard (wait lock) ", args.Num)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Println("kv", kv.me, "(", kv.gid, ") GetShard ", args.Num)
	op := Op{}
	op.Op = GETSHARD
	op.N = args.Num
	op.Id = args.Id
	retval := kv.performPaxosOp(op)
	if retval.ok && retval.isKnown {
		reply.Err = OK
		reply.Value = retval.kvmap
	} else if retval.ok && !retval.isKnown {
		reply.Err = ErrNoSavedShard
	} else {
		reply.Err = ErrWaitingUpdate
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	log.Println("kv", kv.me, "(", kv.gid, ") tick (wait lock) ")
	kv.mu.Lock()
	log.Println("kv", kv.me, "(", kv.gid, ") tick")
	conf := kv.sm.Query(-1)
	if conf.Num != kv.conf.Num { // there's a reconfiguration
		if conf.Num != kv.conf.Num+1 { // make sure to get the first unprocessed reconf
			conf = kv.sm.Query(kv.conf.Num + 1)
		}
		log.Println("kv", kv.me, "(", kv.gid, ") Reconf", kv.conf.Num, "->", conf.Num)
		op := Op{}
		op.Op = RECONF
		op.Conf = conf
		op.Id = int64(op.Conf.Num) // we can take the conf num as paxos operation ID
		// since there will be at most once such reconf
		kv.performPaxosOp(op)
    }
    b := kv.updateNeeded
    kv.mu.Unlock()
    if b {
        for true {
            kv.mu.Lock()
            log.Println("kv", kv.me, "(", kv.gid, ") try to update shards")
            if kv.updateShards() {
                log.Println("kv", kv.me, "(", kv.gid, ") update succeeded")
                kv.updateNeeded = false
                kv.mu.Unlock()
                break
            }
            log.Println("kv", kv.me, "(", kv.gid, ") try to update shards failed - try again")
            kv.mu.Unlock()
        }
    }
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	log.Println("kv*", kv.me, "(", kv.gid, ") killed")
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	kv.kvmap = make(table)
	kv.savepoints = make(map[int]table)

	// Don't call Join().

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

// s : shard index
// num : conf num
func (kv *ShardKV) getShard(servers []string, num int) (table, bool) {
	log.Println("kv", kv.me, "(", kv.gid, ") getShard")
	t := make(table)
	args := &GetShardArgs{}
	args.Key = ""
	args.Num = num
	args.Id = rand.Int63()
	var reply GetShardReply
	for _, srv := range servers {
		log.Println("kv", kv.me, "(", kv.gid, ") ...")
		ok := call(srv, "ShardKV.GetShard", args, &reply)
		if ok && reply.Err == OK {
			log.Println("kv", kv.me, "(", kv.gid, ") getShard succeed")
			t = reply.Value
			return t, true
		} else {
			log.Println("kv", kv.me, "(", kv.gid, ") call to GetShard failed", srv)
		}
	}
	log.Println("kv", kv.me, "(", kv.gid, ") getShard failed")
	return t, false
}

func (kv *ShardKV) updateShards() bool {
	log.Println("kv", kv.me, "(", kv.gid, ") updateShards")
	conf1 := kv.oldConf
	conf2 := kv.conf
	var s [shardmaster.NShards]bool
	// shards i'm responsible in conf2 that i wasn't responsible for in conf1
	for i := 0; i < shardmaster.NShards; i++ {
		s[i] = conf2.Shards[i] == kv.gid && !(conf1.Shards[i] == kv.gid)
	}
	t := make(table)
	for i := 0; i < shardmaster.NShards; i++ {
		if s[i] {
			log.Println("kv", kv.me, "(", kv.gid, ") trying to get shards", i, " from group", conf1.Shards[i], " conf num = ", conf1.Num)
			tmp, ok := kv.getShard(conf1.Groups[conf1.Shards[i]], conf1.Num)
			if !ok {
				log.Println("kv", kv.me, "(", kv.gid, ") error: can't get shard", i)
				return false
			}
			log.Println("kv", kv.me, "(", kv.gid, ") got shards", i)
			for k, v := range tmp {
				if key2shard(k) == i {
					t[k] = v
				}
			}
		}
	}
	log.Println("kv", kv.me, "(", kv.gid, ") got all shards")
	for k, v := range t {
		kv.kvmap[k] = v
	}
	return true
}
