package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type NodeState int8 

const (
	Follower NodeState = iota
	Candidate
	Leader
)
