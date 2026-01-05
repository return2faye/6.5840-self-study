package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Value and Version
type Record struct {
	Value string
	Version rpc.Tversion
}


type KVServer struct {
	mu sync.Mutex
	data map[string]Record
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]Record),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = r.Value
	reply.Version = r.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r, ok := kv.data[args.Key]
	if ok {
		if args .Version != r.Version {
			reply.Err = rpc.ErrVersion
			return
		}

		r.Value = args.Value
		r.Version += 1
		kv.data[args.Key] = r
	} else {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		
		r := Record{
			Value: args.Value,
			Version: 1,
		}
		kv.data[args.Key] = r
	}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
