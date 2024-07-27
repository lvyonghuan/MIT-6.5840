package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	UID       int32
	ClientUID int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	UID       int32
	ClientUID int64
}

type GetReply struct {
	Value string
}
