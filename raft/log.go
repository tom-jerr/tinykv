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
	"math"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	appendIdx uint64 // 接收leader日志后的索引，主要是与leader传来的commit做比较，更新follower的commit index
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	hardState, _, _ := storage.InitialState()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	raftlog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		appendIdx:       math.MaxUint64,
	}
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries
	}
	return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled < firstIndex {
			return l.entries
		}
		if l.stabled-firstIndex >= uint64(len(l.entries)-1) {
			return make([]pb.Entry, 0)
		}
		return l.entries[l.stabled-firstIndex+1:]
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	appliedIndex := l.applied
	commitedIndex := l.committed
	if len(l.entries) > 0 {
		if appliedIndex >= firstIndex-1 && commitedIndex >= firstIndex-1 && appliedIndex < commitedIndex && commitedIndex <= l.LastIndex() {
			// 这里需要注意索引的范围
			return l.entries[appliedIndex-firstIndex+1 : commitedIndex-firstIndex+1]
		}
	}
	return make([]pb.Entry, 0)
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex() // 第一个元素是空的
		return i
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		offset := l.FirstIndex()
		if i >= offset {
			index := i - l.FirstIndex()
			if index >= uint64(len(l.entries)) {
				return 0, ErrUnavailable
			}
			return l.entries[index].Term, nil
		}
	}
	// 所有的日志都被压缩了需要在storage中找
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}

func (l *RaftLog) appliedTo(toApply uint64) {
	l.applied = toApply
}

// 参考 etcd commitTo
func (l *RaftLog) commitTo(toCommit uint64) {
	// never decrease commit
	if l.committed < toCommit {
		//if l.LastIndex() < toCommit {
		//	log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		//}
		l.committed = toCommit
	}
}
