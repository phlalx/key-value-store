package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
    Id    int64
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
    Id int64
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
