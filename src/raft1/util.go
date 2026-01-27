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

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
