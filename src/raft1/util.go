package raft

import "log"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
