package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	lockname string
	myID string
	version rpc.Tversion
}


// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		lockname: l,
		myID: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)

		// Acquired
		if err == rpc.OK && val == lk.myID {
			lk.version = ver
			return
		}

		if err == rpc.ErrNoKey || val == "" {
			putVer := rpc.Tversion(0)
			if err == rpc.OK {
				putVer = ver
			}

			putErr := lk.ck.Put(lk.lockname, lk.myID, putVer)

			if putErr == rpc.OK {
				lk.version = putVer + 1
				return
			}
			// if other status, wait for next round
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		err := lk.ck.Put(lk.lockname, "", lk.version)

		// ErrVersion: released or someome else acquired(non-linearizable)
		if err == rpc.OK || err == rpc.ErrVersion {
			return
		}
	}
}
