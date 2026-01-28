package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func NewElectionTimeOut() time.Duration {
	return time.Duration(300 + rand.Intn(100)) * time.Millisecond
}

// generate copy of slice from start to the end
func generateEntrySlice(start int, origin []LogEntry) []LogEntry {
	new := []LogEntry{}
	copy(new, origin[start:])
	return new
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
