package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) LogLength() (int) {
    return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) SubLog(l int, r int) ([]LogEntry) {
    l -= rf.lastIncludedIndex
    r -= rf.lastIncludedIndex
    return rf.log[l:r]
}

func (rf *Raft) LogTerm(index int) (int) {
    index -= rf.lastIncludedIndex
    if index == 0 {
        return rf.lastIncludedTerm
    } else {
        return rf.log[index - 1].Term
    }
}
