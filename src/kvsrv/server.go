package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type requestInfo struct {
	uid     int32
	content string
}

type KVServer struct {
	mu sync.RWMutex

	rmu sync.RWMutex

	stores  map[string]string
	seenReq map[int64]*requestInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kv.rmu.RLock()
	if value, ok := kv.seenReq[args.ClientUID]; ok {
		kv.rmu.RUnlock()
		if value.uid == args.UID {
			reply.Value = value.content
			return
		}
	} else {
		kv.rmu.RUnlock()
	}

	key := args.Key
	if value, isExist := kv.stores[key]; !isExist {
		reply.Value = ""

		//kv.rmu.Lock()
		//kv.seenReq[args.ClientUID] = &requestInfo{
		//	uid:     args.UID,
		//	content: "",
		//}
		//kv.rmu.Unlock()
		return
	} else {
		reply.Value = value
	}

	//kv.rmu.Lock()
	//kv.seenReq[args.ClientUID] = &requestInfo{
	//	uid:     args.UID,
	//	content: reply.Value,
	//}
	//kv.rmu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rmu.RLock()
	if value, ok := kv.seenReq[args.ClientUID]; ok {
		kv.rmu.RUnlock()
		if value.uid == args.UID {
			reply.Value = value.content
			return
		}
	} else {
		kv.rmu.RUnlock()
	}

	kv.stores[args.Key] = args.Value

	//kv.rmu.Lock()
	//kv.seenReq[args.ClientUID] = &requestInfo{
	//	uid:     args.UID,
	//	content: value,
	//}
	//kv.rmu.Unlock()

	//reply.Value = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rmu.RLock()
	if value, ok := kv.seenReq[args.ClientUID]; ok {
		kv.rmu.RUnlock()
		if value.uid == args.UID {
			reply.Value = value.content
			return
		}
	} else {
		kv.rmu.RUnlock()
	}

	key := args.Key
	value := args.Value
	reply.Value = kv.stores[key]
	kv.stores[key] += value

	kv.rmu.Lock()
	kv.seenReq[args.ClientUID] = &requestInfo{
		uid:     args.UID,
		content: reply.Value,
	}
	kv.rmu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.stores = make(map[string]string)
	kv.seenReq = make(map[int64]*requestInfo)

	return kv
}
