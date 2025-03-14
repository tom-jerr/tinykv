// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const debug bool = false

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	heartBeats map[uint64]bool // 记录已经发送了心跳包的节点，主要判断是否节点已经变成孤岛
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// 最开始的term, note, peers都在稳定存储中
	hardstate, confstate, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	var raft = &Raft{
		id:               c.ID,
		Term:             hardstate.Term,
		Vote:             hardstate.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	if c.Applied > 0 {
		raft.RaftLog.appliedTo(c.Applied)
	}
	// 初始化peers的progress, next为当前log的最后一个index+1，match为0
	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// leader-only, 从leader的firstindex开始发送
	if _, ok := r.Prs[to]; !ok {
		return false

	}
	if debug {
		fmt.Printf("%x send append to %x at term %d\n", r.id, to, r.Term)
	}
	prevLogIndex := r.Prs[to].Next - 1               // 紧邻新日志条目之前的那个日志条目的索引
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex) // 紧邻新日志条目之前的那个日志条目的任期

	firstIndex := r.RaftLog.FirstIndex()
	if err != nil || prevLogIndex < firstIndex-1 {
		// 如果prevLogIndex小于firstIndex-1，说明prevLogIndex之前的日志条目已经被压缩了
		// TODO(LZY): send snapshot
		return true
	}
	var entries []*pb.Entry
	n := r.RaftLog.LastIndex() + 1
	for i := prevLogIndex + 1; i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	if debug {
		fmt.Printf("%x send raft message %s from %x to %x\n", r.id, pb.MessageType_MsgAppend, r.id, to)
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  util.RaftInvalidIndex, // Msg 的初始化检查要求
	}
	r.msgs = append(r.msgs, msg)

}

// sendRequestVote sends an vote RPC to the given peer.
// Returns true if a message was sent.
func (r *Raft) sendRequestVote(to uint64) bool {
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	lastLogIndex := r.RaftLog.LastIndex()          // 候选人的最后日志条目的索引值
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex) // 候选人最后日志条目的任期号

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
		Entries: nil,
	}
	r.msgs = append(r.msgs, msg)
	if debug {
		fmt.Printf("%x send raft message %s from %x to %x\n", r.id, pb.MessageType_MsgRequestVote, r.id, to)
	}
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if debug {
			fmt.Println(r.id, "term:", r.Term, "StateFollower", "r.electionElapsed:", r.electionElapsed, "peers num:", len(r.Prs))
		}
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			// 发起投票
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if debug {
			fmt.Println(r.id, "term:", r.Term, "StateCandidate", "r.electionElapsed:", r.electionElapsed, "peers num:", len(r.Prs))
		}
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			// 发起投票
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		if debug {
			fmt.Println(r.id, "term:", r.Term, "StateLeader", "r.heartbeatTimeout:", r.heartbeatTimeout, "peers num:", len(r.Prs))
		}
		r.electionElapsed++
		num := len(r.heartBeats)
		length := len(r.Prs) / 2
		// leader 超时，要么是其他节点挂了，要么是自己成为孤岛，通过之前发送的心跳包来判断
		if r.electionElapsed >= r.heartbeatTimeout {
			r.electionElapsed = 0 - rand.Intn(r.heartbeatTimeout)
			r.heartBeats = make(map[uint64]bool)
			r.heartBeats[r.id] = true
			if num <= length {
				// 重新发起选举
				r.campaign()
			}
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}
}

// Handle the campaign to start a new election
// Once 'campaign' method is called, the node becomes candidate
// and sends `MessageType_MsgRequestVote` to peers in cluster to request votes.
func (r *Raft) campaign() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.becomeCandidate()
	// 如果集群中只有自己一个节点，则直接成为leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if debug {
		fmt.Printf("%x became follower at term %x\n", r.id, r.Term)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 增加任期并尝试给自己投票
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if debug {
		fmt.Printf("%x became candidate at term %x\n", r.id, r.Term)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.leadTransferee = None
	r.Lead = r.id
	r.heartBeats = make(map[uint64]bool)
	r.heartBeats[r.id] = true
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer != r.id {
			r.Prs[peer].Next = lastIndex + 1
			r.Prs[peer].Match = 0
		}
	}

	// 提交一个空的entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next++
	if debug {
		fmt.Printf("%x became leader at term %x\n", r.id, r.Term)
	}

	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	// 如果集群中只有一个节点
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if debug {
		fmt.Printf("%x handle raft message %s from %x to %x at term %x\n", r.id, m.MsgType, m.From, m.To, m.Term)
		if _, ok := r.Prs[r.id]; !ok {
			fmt.Printf("%x not exist in r.Prs\n", r.id)
		}
	}
	switch r.State {
	case StateFollower:
		err := r.FollowerStep(m)
		if err != nil {
			return err
		}
	case StateCandidate:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		err := r.CandidateStep(m)
		if err != nil {
			return err
		}
	case StateLeader:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		err := r.LeaderStep(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: // leader only
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader: //leader only
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat: //leader only
	case pb.MessageType_MsgPropose: //dropped
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: //leader only
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: // Candidate only
		if debug {
			fmt.Printf("[%d] receive vote response from %d, reject=%v\n", r.id, m.From, m.Reject)
		}
		r.votes[m.From] = !m.Reject
		length := len(r.Prs) / 2
		grant := 0
		denials := 0
		// 统计投票结果, 如果有超过半数的节点投票给自己，则成为leader
		for _, status := range r.votes {
			if status {
				grant++
			} else {
				denials++
			}
		}
		if grant > length {
			r.becomeLeader()
		} else if denials > length {
			r.becomeFollower(r.Term, m.From)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: //leader only
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow: // leader迁移时，当新旧leader的日志数据同步后，旧leader向新leader发送该消息通知可以进行迁移了
		r.campaign()
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat: // 通知leader发送心跳包
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgPropose: // 处理上层传来的数据
		if r.leadTransferee == None {
			r.handleMsgPropose(m)
		}
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // 处理follower返回的append response
		r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgRequestVote: // 处理投票请求
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: //Candidate only
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat: //error
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: // leader only，Commit 字段用于告诉 Leader 自己是否落后
		r.heartBeats[m.From] = true
		if debug {
			println(r.id, "MsgHeartbeatResponse, m.Commit:", m.Commit,
				"r.RaftLog.committed:", r.RaftLog.committed, "m.Reject:", m.Reject)
		}
		if m.Commit < r.RaftLog.committed {
			// 如果follower的commit小于leader的commit，说明follower落后了，需要leader发送数据给follower
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader: // 转换leader
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}
func (r *Raft) handleMsgPropose(m pb.Message) {
	// append entries to leader
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// broadcast append entries to followers
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	// 集群中只有leader
	if len(r.Prs) == 1 {
		r.RaftLog.commitTo(r.Prs[r.id].Match)
	}
}
func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// if debug {
	// 	fmt.Printf("%x handleAppendEntries from %x at term %x\n", r.id, m.From, m.Term)
	// }

	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}
	// change state
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	if r.State == StateLeader {
		return
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}

	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm

	// 返回假, 如果超范围
	if prevLogIndex > r.RaftLog.LastIndex() {
		r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}
	// 返回假，如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	if tmpTerm, _ := r.RaftLog.Term(prevLogIndex); tmpTerm != prevLogTerm {
		r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}
	// 追加新条目，同时删除冲突
	for _, en := range m.Entries {
		index := en.Index
		oldTerm, err := r.RaftLog.Term(index)
		if index-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *en)
		} else if oldTerm != en.Term || err != nil {
			// 不匹配，删除从此往后的所有条目
			if index < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else {
				r.RaftLog.entries = r.RaftLog.entries[0 : index-r.RaftLog.FirstIndex()]
			}
			// 更新stable
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			// 追加新条目
			r.RaftLog.entries = append(r.RaftLog.entries, *en)

		}
	}

	r.RaftLog.appendIdx = m.Index + uint64(len(m.Entries))

	// 返回真
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
	// 更新commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.appendIdx)
	}

}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	// if debug {
	// 	fmt.Printf("%x handleMsgAppendResponse from %x at term %x\n", r.id, m.From, m.Term)
	// }
	// 同步failed
	if m.Reject {
		// 可能第一次尝试失败，next为0，索引去m.index加1与next-1的最小值
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return
	}
	// 同步成功
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// check if can TransferLeader
	if m.From == r.leadTransferee {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From})
	}
	// 假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，则令 commitIndex = N
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, peer := range r.Prs {
		match[i] = peer.Match
		i++
	}
	sort.Sort(match)
	Match := match[(len(r.Prs)-1)/2]
	matchTerm, _ := r.RaftLog.Term(Match)
	// println("match:", Match, "r.RaftLog.committed:", r.RaftLog.committed)
	// Raft 永远不会通过计算副本数目的方式去提交一个之前任期内的日志条目
	if Match > r.RaftLog.committed && matchTerm == r.Term {
		r.RaftLog.committed = Match

		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}

	}

}

func (r *Raft) handleRequestVote(m pb.Message) {
	// if debug {
	// 	fmt.Printf("%x handleRequestVote from %x at term %x\n", r.id, m.From, m.Term)
	// }
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
	}
	// If votedFor is null or candidateId
	if r.Vote == None || r.Vote == m.From {
		// 只有候选人任期更大，或者任期相同但是日志更新的情况下才会投票
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm > lastTerm ||
			(m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	r.msgs = append(r.msgs, msg)

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// if debug {
	// 	fmt.Printf("%x handleHeartbeat from %x at term %x\n", r.id, m.From, m.Term)
	// }
	// 拒绝任期小于自己的心跳包
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Commit:  r.RaftLog.committed,
			Index:   r.RaftLog.stabled,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}

	r.electionElapsed -= r.heartbeatTimeout
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.stabled,
	}
	r.msgs = append(r.msgs, msg)
}

// handle TransferLeader, leader only
func (r *Raft) handleTransferLeader(m pb.Message) {
	// if debug {
	// 	fmt.Printf("%x handleTransferLeader from %x at term %x\n", r.id, m.From, m.Term)
	// }
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	// 日志相同，发送可以转移的消息，否则先进行append entrie
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
func (r *Raft) GetID() uint64 {
	return r.id
}
