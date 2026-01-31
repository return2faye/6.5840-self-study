package kvraft

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

const maxRetries = 3 // after this many consecutive !ok to same server, try next (avoids dead loop if server killed)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	mu      sync.Mutex
	leader  int // hint: last server that successfully handled a request
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	retries := 0
	for {
		ck.mu.Lock()
		i := ck.leader
		ck.mu.Unlock()

		args := rpc.GetArgs{Key: key}
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)

		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		}
		if ok && reply.Err == rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			retries = 0
			continue
		}
		// !ok: transient failure or server dead; after maxRetries try next server
		retries++
		if retries >= maxRetries {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			retries = 0
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	rpcsSent := 0
	retries := 0
	for {
		rpcsSent++
		ck.mu.Lock()
		i := ck.leader
		ck.mu.Unlock()

		args := rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)

		if !ok {
			retries++
			if retries >= maxRetries {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				retries = 0
			}
			continue
		}
		retries = 0
		if reply.Err == rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			continue
		}

		// Got a definitive response from the leader (OK or ErrVersion)
		if reply.Err == rpc.OK {
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()
			return rpc.OK
		}
		if reply.Err == rpc.ErrVersion {
			if rpcsSent == 1 {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
	}
}
