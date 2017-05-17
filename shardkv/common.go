package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
    ErrNoSavedShard    = "ErrNoSavedShard"
    ErrWaitingUpdate    = "ErrWaitingUpdate"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
    Id    int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
    Id  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	Key string
    Num int
    Id  int64
}

type GetShardReply struct {
	Err   Err
	Value map[string]string 
}
