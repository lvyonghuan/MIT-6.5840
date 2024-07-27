package kvsrv

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd

	uid       int32
	clientUID int64
}

func nRand() int64 {
	maxNum := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, maxNum)
	x := bigX.Int64() + 1 //不生成0
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientUID = nRand()
	ck.uid = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.uid++

	var (
		isSuccess = false
		reply     = &GetReply{}
	)

	for !isSuccess {
		isSuccess = ck.server.Call("KVServer.Get", &GetArgs{Key: key, UID: ck.uid, ClientUID: ck.clientUID}, reply)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.uid++

	var (
		isSuccess = false
		reply     = &PutAppendReply{}
	)

	for !isSuccess {
		isSuccess = ck.server.Call("KVServer."+op, &PutAppendArgs{Key: key, Value: value, UID: ck.uid, ClientUID: ck.clientUID}, reply)
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
