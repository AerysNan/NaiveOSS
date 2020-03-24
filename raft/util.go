package raft

import (
	"fmt"
	"math/rand"
	pr "oss/proto/raft"
	"time"
)

func StateToString(state int) string {
	switch state {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return ""
}

func PointerArrayToArray(entries []*pr.LogEntry) []pr.LogEntry {
	result := make([]pr.LogEntry, 0)
	for _, e := range entries {
		result = append(result, *e)
	}
	return result
}

func ArrayToPointerArray(entries []pr.LogEntry) []*pr.LogEntry {
	result := make([]*pr.LogEntry, 0)
	for i := 0; i < len(entries); i++ {
		result = append(result, &entries[i])
	}
	return result
}

func EntriesToString(entries []pr.LogEntry) string {
	s := ""
	for _, entry := range entries {
		s += fmt.Sprintf("%v ", entry.Term)
	}
	return fmt.Sprintf("{%v}", s)
}

func RandomRange(min int, max int) time.Duration {
	return time.Duration(rand.Intn(max-min) + min)
}
