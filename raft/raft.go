package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	pr "oss/proto/raft"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/sirupsen/logrus"
)

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

const (
	HeartbeatTimeout = 120
	ElectTimeoutMin  = 250
	ElectTimeoutMax  = 400
)

type Raft struct {
	mu        sync.Mutex      // Lock to protect shared access to this peer's state
	peers     []pr.RaftClient // RPC end points of all peers
	persister *Persister      // Object to hold this peer's persisted state
	me        int             // this peer's index into peers[]

	currentTerm int
	votedFor    int
	state       int

	logs          []pr.LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	snapshotIndex int

	applyCh chan pr.Message

	electTimer *time.Timer
	heartTimer *time.Timer
	logger     *logrus.Logger
}

func (rf *Raft) SetConnections(me int, peers []pr.RaftClient) {
	rf.me = me
	rf.peers = peers
}

func (rf *Raft) toAbsolute(index int) int {
	return index + rf.snapshotIndex
}

func (rf *Raft) toRelative(index int) int {
	return index - rf.snapshotIndex
}

func (rf *Raft) transitState(state int) {
	if rf.state == state {
		return
	}
	rf.logger.Debugf("[%vT%vS%v] transit from state %v to state %v", rf.me, rf.currentTerm, rf.snapshotIndex, StateToString(rf.state), StateToString(state))
	switch state {
	case StateFollower:
		rf.heartTimer.Stop()
		rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
		rf.votedFor = -1
		rf.persist()
	case StateCandidate:
	case StateLeader:
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.toAbsolute(len(rf.logs))
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.snapshotIndex
		}
		rf.electTimer.Stop()
		rf.heartTimer.Reset(HeartbeatTimeout * time.Millisecond)
	}
	rf.state = state
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm, votedFor, snapshotIndex int
	var logs []pr.LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&logs) != nil {
		rf.logger.Warnf("[%vT%vS%v] read persist failed", rf.me, rf.currentTerm, rf.snapshotIndex)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.snapshotIndex = snapshotIndex
	rf.lastApplied = snapshotIndex
	rf.commitIndex = snapshotIndex
}

func (rf *Raft) startElection() {
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	request := &pr.VoteRequest{
		Term:         int64(rf.currentTerm),
		CandidateId:  int64(rf.me),
		LastLogIndex: int64(rf.toAbsolute(len(rf.logs) - 1)),
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	total := 1
	rf.votedFor = rf.me
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(receiver int) {
			response, err := rf.peers[receiver].Vote(context.Background(), request)
			if err == nil {
				rf.mu.Lock()
				if response.VoteGranted && rf.state == StateCandidate {
					total += 1
					if total > len(rf.peers)/2 && rf.state == StateCandidate {
						rf.transitState(StateLeader)
						rf.broadcastHeartbeat()
					}
				} else if int(response.Term) > rf.currentTerm {
					rf.currentTerm = int(response.Term)
					rf.transitState(StateFollower)
				}
				rf.mu.Unlock()
			} else {
				rf.logger.WithError(err).Warnf("[%vT%vS%v] request vote from %v failed", rf.me, rf.currentTerm, rf.snapshotIndex, receiver)
			}
		}(i)
	}
}

func (rf *Raft) Vote(ctx context.Context, request *pr.VoteRequest) (*pr.VoteResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.logger.Debugf("[%vT%vS%v] receive request vote from [%vT%v]", rf.me, rf.currentTerm, rf.snapshotIndex, request.CandidateId, request.Term)
	end := len(rf.logs) - 1
	if rf.currentTerm > int(request.Term) ||
		(rf.currentTerm == int(request.Term) && rf.votedFor != -1) {
		return &pr.VoteResponse{
			Term:        int64(rf.currentTerm),
			VoteGranted: false,
		}, nil
	}
	if int(request.Term) > rf.currentTerm {
		rf.currentTerm = int(request.Term)
		rf.transitState(StateFollower)
	}
	if rf.logs[end].Term > request.LastLogTerm ||
		(rf.logs[end].Term == request.LastLogTerm && int(request.LastLogIndex) < rf.toAbsolute(end)) {
		return &pr.VoteResponse{
			Term:        int64(rf.currentTerm),
			VoteGranted: false,
		}, nil
	}
	rf.votedFor = int(request.CandidateId)
	rf.currentTerm = int(request.Term)
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	rf.transitState(StateFollower)
	return &pr.VoteResponse{
		Term:        int64(rf.currentTerm),
		VoteGranted: true,
	}, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, request *pr.AppendRequest) (*pr.AppendResponse, error) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.persist()
	}()
	entriesToAppend := PointerArrayToArray(request.Entries)
	rf.logger.Debugf("[%vT%vS%v] with logs %v receive append entries %v from [%vT%v]", rf.me, rf.currentTerm, rf.snapshotIndex, EntriesToString(rf.logs), EntriesToString(entriesToAppend), request.LeaderId, request.Term)
	if rf.currentTerm > int(request.Term) {
		rf.logger.Debugf("[%vT%vS%v] reject append entries due to term", rf.me, rf.currentTerm, rf.snapshotIndex)
		return &pr.AppendResponse{
			Term:    int64(rf.currentTerm),
			Success: false,
		}, nil
	}
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	if int(request.PrevLogIndex) <= rf.snapshotIndex {
		rf.logger.Debugf("[%vT%vS%v] previous index %v before snapshot index %v", rf.me, rf.currentTerm, rf.snapshotIndex, request.PrevLogIndex, rf.snapshotIndex)
		if int(request.PrevLogIndex)+len(request.Entries) > rf.snapshotIndex {
			rf.logs = rf.logs[:1]
			rf.logs = append(rf.logs, entriesToAppend[rf.snapshotIndex-int(request.PrevLogIndex):]...)
		}
		if int(request.LeaderCommit) > rf.commitIndex {
			end := rf.toAbsolute(len(rf.logs) - 1)
			if int(request.LeaderCommit) <= end {
				rf.commitIndex = int(request.LeaderCommit)
				rf.logger.Debugf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, request.LeaderCommit)
			} else {
				rf.commitIndex = end
				rf.logger.Debugf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, end)
			}
			rf.apply()
		}
		return &pr.AppendResponse{
			Term:    int64(rf.currentTerm),
			Success: true,
		}, nil
	}
	if rf.toAbsolute(len(rf.logs)-1) < int(request.PrevLogIndex) ||
		rf.logs[rf.toRelative(int(request.PrevLogIndex))].Term != request.PrevLogTerm {
		response := &pr.AppendResponse{
			Term:    int64(rf.currentTerm),
			Success: false,
		}
		if rf.toAbsolute(len(rf.logs)-1) < int(request.PrevLogIndex) {
			response.ConflictIndex = int64(rf.toAbsolute(len(rf.logs)))
			response.ConflictTerm = -1
		} else {
			response.ConflictTerm = rf.logs[rf.toRelative(int(request.PrevLogIndex))].Term
			index := int(request.PrevLogIndex)
			for rf.logs[rf.toRelative(index-1)].Term == response.ConflictTerm && index > rf.snapshotIndex+1 {
				index--
			}
			response.ConflictIndex = int64(index)
		}
		rf.logger.Debugf("[%vT%vS%v] reject append entries due to log mismatch", rf.me, rf.currentTerm, rf.snapshotIndex)
		return response, nil
	}
	for index, entry := range request.Entries {
		if len(rf.logs) <= rf.toRelative(int(request.PrevLogIndex)+index+1) ||
			rf.logs[rf.toRelative(int(request.PrevLogIndex)+index+1)].Term != entry.Term {
			rf.logs = append(rf.logs[:rf.toRelative(int(request.PrevLogIndex)+index+1)], entriesToAppend[index:]...)
			break
		}
	}
	if int(request.LeaderCommit) > rf.commitIndex {
		end := rf.toAbsolute(len(rf.logs) - 1)
		if int(request.LeaderCommit) <= end {
			rf.commitIndex = int(request.LeaderCommit)
			rf.logger.Debugf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, request.LeaderCommit)
		} else {
			rf.commitIndex = end
			rf.logger.Debugf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, end)
		}
		rf.apply()
	}
	rf.currentTerm = int(request.Term)
	response := &pr.AppendResponse{
		Term:    request.Term,
		Success: true,
	}
	rf.transitState(StateFollower)
	return response, nil
}

func (rf *Raft) InstallSnapshot(ctx context.Context, request *pr.SnapshotRequest) (*pr.SnapshotResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if int(request.Term) < rf.currentTerm || int(request.LastIncludedIndex) < rf.snapshotIndex {
		return &pr.SnapshotResponse{
			Term: int64(rf.currentTerm),
		}, nil
	}
	if int(request.Term) > rf.currentTerm {
		rf.currentTerm = int(request.Term)
		rf.transitState(StateFollower)
	}
	index := rf.toRelative(int(request.LastIncludedIndex))
	if len(rf.logs) > index &&
		rf.logs[index].Term == request.LastIncludedTerm {
		rf.logs = rf.logs[index:]
	} else {
		rf.logs = []pr.LogEntry{{Term: request.LastIncludedTerm, Command: nil}}
	}
	rf.logger.Infof("[%vT%vS%v] install snapshot to index %v", rf.me, rf.currentTerm, rf.snapshotIndex, request.LastIncludedIndex)
	rf.snapshotIndex = int(request.LastIncludedIndex)
	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}
	rf.logger.Debugf("[%vT%vS%v] commit index now %v after install snapshot", rf.me, rf.currentTerm, rf.snapshotIndex, rf.commitIndex)
	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), request.Data)
	if rf.lastApplied > rf.snapshotIndex {
		return &pr.SnapshotResponse{
			Term: int64(rf.currentTerm),
		}, nil
	}
	args := pr.SnapshotRequest{
		Data: rf.persister.ReadSnapshot(),
	}
	any, err := ptypes.MarshalAny(&args)
	if err != nil {
		return nil, err
	}
	command := pr.Message{
		CommandValid: true,
		CommandIndex: int64(rf.snapshotIndex),
		Command: &pr.Op{
			Type: pr.Type_SNAPSHOT,
			Args: any,
		},
	}
	go func(msg pr.Message) {
		rf.applyCh <- msg
	}(command)
	return &pr.SnapshotResponse{
		Term: int64(rf.currentTerm),
	}, nil
}

func (rf *Raft) apply() {
	if rf.commitIndex <= rf.lastApplied {
		return
	}
	entries := rf.logs[rf.toRelative(rf.lastApplied+1):rf.toRelative(rf.commitIndex+1)]
	c := make([]pr.LogEntry, len(entries))
	copy(c, entries)
	go func(start int, entries []pr.LogEntry) {
		for offset, entry := range entries {
			rf.applyCh <- pr.Message{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int64(start + offset),
			}
			rf.mu.Lock()
			rf.logger.Infof("[%vT%vS%v] send command %v to channel", rf.me, rf.currentTerm, rf.snapshotIndex, entry.Command)
			if rf.lastApplied < start+offset {
				rf.lastApplied = start + offset
			}
			rf.mu.Unlock()
		}
	}(rf.lastApplied+1, c)
	rf.logger.Debugf("[%vT%vS%v] apply entries %v-%v", rf.me, rf.currentTerm, rf.snapshotIndex, rf.lastApplied+1, rf.commitIndex)
}

func (rf *Raft) Start(command *pr.Op) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		rf.logger.Infof("[%vT%vS%v] start command %v", rf.me, rf.currentTerm, rf.snapshotIndex, command)
		index = rf.toAbsolute(len(rf.logs))
		rf.logs = append(rf.logs, pr.LogEntry{
			Command: command,
			Term:    int64(term),
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.mu.Unlock()
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.broadcastHeartbeat()
		}()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.logger.Debugf("[%vT%vS%v] killed", rf.me, rf.currentTerm, rf.snapshotIndex)
}

func (rf *Raft) broadcastHeartbeat() {
	rf.logger.Debugf("[%vT%vS%v] broadcast heartbeat with logs %v", rf.me, rf.currentTerm, rf.snapshotIndex, EntriesToString(rf.logs))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(receiver int) {
			rf.mu.Lock()
			if rf.state != StateLeader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[receiver] - 1
			if prevLogIndex < rf.snapshotIndex {
				rf.mu.Unlock()
				rf.sendSnapshot(receiver)
				return
			}
			entries := make([]pr.LogEntry, len(rf.logs[rf.toRelative(prevLogIndex+1):]))
			copy(entries, rf.logs[rf.toRelative(prevLogIndex+1):])
			request := &pr.AppendRequest{
				Term:         int64(rf.currentTerm),
				LeaderId:     int64(rf.me),
				PrevLogIndex: int64(prevLogIndex),
				PrevLogTerm:  rf.logs[rf.toRelative(prevLogIndex)].Term,
				Entries:      ArrayToPointerArray(entries),
				LeaderCommit: int64(rf.commitIndex),
			}
			rf.mu.Unlock()
			response, err := rf.peers[receiver].AppendEntries(context.Background(), request)
			if err == nil {
				rf.mu.Lock()
				if rf.state != StateLeader {
					rf.mu.Unlock()
					return
				}
				if response.Success {
					rf.matchIndex[receiver] = int(request.PrevLogIndex) + len(request.Entries)
					rf.nextIndex[receiver] = rf.matchIndex[receiver] + 1
					for j := rf.toAbsolute(len(rf.logs) - 1); j > rf.commitIndex; j-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= j {
								count += 1
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = j
							rf.logger.Debugf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, j)
							rf.apply()
							break
						}
					}
				} else {
					if int(response.Term) > rf.currentTerm {
						rf.currentTerm = int(response.Term)
						rf.transitState(StateFollower)
						rf.persist()
					} else {
						rf.nextIndex[receiver] = int(response.ConflictIndex)
						if response.ConflictTerm != -1 {
							for i := int(request.PrevLogIndex); i >= rf.snapshotIndex+1; i-- {
								if rf.logs[rf.toRelative(i-1)].Term == response.ConflictTerm {
									rf.nextIndex[receiver] = i
									break
								}
							}
						}
					}
				}
				rf.mu.Unlock()
			} else {
				rf.logger.WithError(err).Warnf("[%vT%vS%v] send append entries to %v failed", rf.me, rf.currentTerm, rf.snapshotIndex, receiver)
			}
		}(i)
	}
}

func (rf *Raft) StartTimer() {
	for {
		select {
		case <-rf.electTimer.C:
			rf.mu.Lock()
			rf.logger.Debugf("[%vT%vS%v] election timer timeout", rf.me, rf.currentTerm, rf.snapshotIndex)
			rf.transitState(StateCandidate)
			rf.currentTerm += 1
			rf.persist()
			rf.startElection()
			rf.mu.Unlock()
		case <-rf.heartTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.broadcastHeartbeat()
				rf.heartTimer.Reset(HeartbeatTimeout * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.snapshotIndex)
	_ = e.Encode(rf.logs)
	return w.Bytes()
}

func (rf *Raft) ReplaceLogWithSnapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		rf.logger.Debugf("[%vT%vS%v] reject replace log with snapshot index %v current %v", rf.me, rf.currentTerm, rf.snapshotIndex, index, rf.snapshotIndex)
		return
	}
	rf.logger.Debugf("[%vT%vS%v] replace log with snapshot until index %v", rf.me, rf.currentTerm, rf.snapshotIndex, index)
	rf.logs = rf.logs[rf.toRelative(index):]
	rf.snapshotIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), data)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendSnapshot(i)
	}
}

func (rf *Raft) sendSnapshot(receiver int) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	request := &pr.SnapshotRequest{
		Term:              int64(rf.currentTerm),
		LeaderId:          int64(rf.me),
		LastIncludedIndex: int64(rf.snapshotIndex),
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	response, err := rf.peers[receiver].InstallSnapshot(context.Background(), request)
	if err == nil {
		rf.mu.Lock()
		if int(response.Term) > rf.currentTerm {
			rf.currentTerm = int(response.Term)
			rf.transitState(StateFollower)
			rf.persist()
		} else {
			if rf.matchIndex[receiver] < int(request.LastIncludedIndex) {
				rf.matchIndex[receiver] = int(request.LastIncludedIndex)
			}
			rf.nextIndex[receiver] = rf.matchIndex[receiver] + 1
		}
		rf.mu.Unlock()
	} else {
		rf.logger.WithError(err).Warnf("[%vT%vS%v] send snapshot to %v failed", rf.me, rf.currentTerm, rf.snapshotIndex, receiver)
	}
}

func Make(peers []pr.RaftClient, me int, persister *Persister, applyCh chan pr.Message) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = StateFollower
	rf.applyCh = applyCh
	rf.logs = make([]pr.LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.logger = logrus.New()
	rf.logger.SetLevel(logrus.ErrorLevel)
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartTimer = time.NewTimer(HeartbeatTimeout * time.Millisecond)
	rf.electTimer = time.NewTimer(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	return rf
}

func (rf *Raft) SetLogLevel(level logrus.Level) {
	rf.logger.SetLevel(level)
}
