package rsm

import (
	"reflect"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

var useRaftStateMachine bool // to plug in another raft besided raft1


// Op is the command replicated through Raft. Req is the actual operation
// (e.g. Inc{}, Null{}) that the state machine will execute in DoOp(Req).
type Op struct {
	Id int
	Req any // the request to execute; must be a registered labgob type
	Me int
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// index -> waiter: only send result if applied op matches submitted op
	pending map[int]struct {
		ch chan any
		op Op
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]struct{ ch chan any; op Op }),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

// applier reads committed entries from applyCh and applies them to the state machine.
// Exits when applyCh is closed (e.g. after Raft Kill()).
func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			appliedOp := msg.Command.(Op)
			result := rsm.sm.DoOp(appliedOp.Req)
			rsm.mu.Lock()
			entry, ok := rsm.pending[msg.CommandIndex]
			if ok {
				delete(rsm.pending, msg.CommandIndex)
			}
			rsm.mu.Unlock()
			// only send result if the applied command is the one this Submit submitted
			if ok && reflect.DeepEqual(entry.op, appliedOp) {
				entry.ch <- result
			}
		}
		// SnapshotValid (4C): handle later
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	op := Op{Req: req}
	index, startTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	rsm.mu.Lock()
	rsm.pending[index] = struct{ ch chan any; op Op }{ch, op}
	rsm.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
waitLoop:
	for time.Now().Before(deadline) {
		select {
		case result := <-ch:
			rsm.mu.Lock()
			delete(rsm.pending, index)
			rsm.mu.Unlock()
			return rpc.OK, result
		case <-time.After(50 * time.Millisecond):
			term, leader := rsm.rf.GetState()
			if !leader || term != startTerm {
				break waitLoop
			}
		}
	}
	rsm.mu.Lock()
	delete(rsm.pending, index)
	rsm.mu.Unlock()
	return rpc.ErrWrongLeader, nil
}
