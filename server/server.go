package server

import (
	"fmt"
	"github.com/pingcap/tidb/store/tikv"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"os"
	kv "server.go/KVService"
	"sync"
	"time"
)

const (
	PeerNum = 3
	debug = true
	electionTime = 150
	heartbeatInterval = 10
	checkInterval = 30
	maxSharedChan = 16
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

type Server struct {
	mu 				sync.Mutex
	cli 			*tikv.RawKVClient
	peerHandlers 	[]kv.HandlerClient

	// election
	sid 			int64
	state 			State
	curTerm 		int64
	votedFor 		int64
	leaderId		int64
	electionResetEvent time.Time

	// logging
	log 			[]kv.LogEntry
	commitChan 		chan CommitEntry	// commit entry to deal with
	newCommitReadyChan chan struct{} // receive an empty struct as signal to deliver new commit
	commitIndex 	int64
	lastApplied		int64
	curSharedChanId int64

	nextIndex		map[int]int64
	matchIndex		map[int]int64
	sharedChan 		[maxSharedChan]chan kv.KV
}

type ServerSetting struct {
	Cli 			*tikv.RawKVClient
	PeerHandlers 	[]kv.HandlerClient

	LocalIP 		string
	LocalPort 		string
	PeerIP 			[]string
	PeerPorts 		[]string
	Sid 			int64
}

// CommitEntry contains committed log
type CommitEntry struct {
	Op			kv.OperationType
	KeyVal 		kv.KV
	Index		int64
	Term 		int64
	ChanId 		int64
}

///////////////////////////////////////////////////////////////////////////////////////////////
// auxiliary methods

func (s *Server) Set(setting ServerSetting) {
	fmt.Println("Begining initializing")
	s.cli = setting.Cli
	s.peerHandlers = setting.PeerHandlers
	s.sid = setting.Sid
	s.state = Dead
	s.leaderId = -1

	s.newCommitReadyChan = make(chan struct{}, maxSharedChan) // message queue
	s.commitIndex = -1
	s.lastApplied = -1
	s.curSharedChanId = 0
	s.nextIndex = make(map[int]int64)
	s.matchIndex = make(map[int]int64)
	for i := 0; i < maxSharedChan; i ++ {
		s.sharedChan[i] = make(chan kv.KV)
	}
	s.commitChan = make(chan CommitEntry)
}

func (s *Server) Start() {
	if s.state == Dead {
		go s.becomeFollower(0)
	}
	go s.commitChanSender()
	go s.handleCommitEntry()
}

func (s *Server) GetLeaderID(ctx context.Context, id *kv.LeaderID) (*kv.LeaderID, error) {
	s.mu.Lock()
	id.Id = s.leaderId
	s.mu.Unlock()
	return id, nil
}

func (s *Server) GetState(ctx context.Context, sta *kv.State) (*kv.State, error) {
	s.mu.Lock()
	sta.State = int64(s.state)
	s.mu.Unlock()
	return sta, nil
}

func (s *Server) debugLog(format string, args ...interface{}) {
	if debug {
		format = fmt.Sprintf("[%d] ", s.sid) + format
		log.Printf(format, args)
	}
}

func (s *Server) Report() (id int64, term int64, isLeader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sid, s.curTerm, s.state == Leader
}

func (s *Server) lastLogIndexAndTerm() (int64, int64) {
	if len(s.log) == 0 {
		return -1, -1
	} else {
		lastIndex := int64(len(s.log) - 1)
		return lastIndex, s.log[lastIndex].Term
	}
}

func minInt(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Election, heartbeats and log replication

// RequestVote RPC: handle vote request from candidate
func (s *Server) RequestVote(ctx context.Context, args *kv.RequestVoteArgs) (*kv.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply := kv.RequestVoteReply{}
	lastLogIndex, lastLogTerm := s.lastLogIndexAndTerm()
	s.debugLog("Received vote request from %d, term %d\n", args.CandidateId, args.Term)

	// current term is out of date
	if s.curTerm < args.Term {
		s.debugLog("Current term %d < %d\n", s.curTerm, args.Term)
		s.becomeFollower(args.Term)
	}

	// check whether to vote for(check curTerm and log term and index)
	if s.curTerm == args.Term &&
		(s.votedFor == -1 || s.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)){
		s.votedFor = args.CandidateId
		reply.VoteGranted = true
		s.electionResetEvent = time.Now()
		s.debugLog("Vote for candidate %d\n", args.CandidateId)
	} else {
		reply.VoteGranted = false
		s.debugLog("Refuse vote request from %d", args.CandidateId)
	}
	reply.Term = s.curTerm
	return &reply, nil
}

func (s *Server) AppendEntries(ctx context.Context, args *kv.AppendEntriesArg) (*kv.AppendEntriesReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply := kv.AppendEntriesReply{}
	//s.debugLog("Received AppendEntries request from leader %d\n", args.LeaderId)

	// current term is out of date
	if s.curTerm < args.Term {
		s.debugLog("Current term %d < %d in AppendEntries\n", s.curTerm, args.Term)
		s.becomeFollower(args.Term)
	}

	// handle heartbeat message
	s.leaderId = args.LeaderId
	reply.Success = false
	if s.curTerm == args.Term {
		// restrain candidate to be a follower
		if s.state != Follower {
			s.becomeFollower(args.Term)
		}
		s.electionResetEvent = time.Now()

		// replicate log from leader
		// if s hasn't been logged or has new matched logs
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < int64(len(s.log)) && args.PrevLogTerm == s.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// indexing position to be logged and to log
			logInsertIndex := int(args.PrevLogIndex + 1)
			newEntriesIndex := 0
			for {
				// out of range
				if logInsertIndex >= len(s.log) || newEntriesIndex >= len(args.Entries) {
					break
				}

				// terms not matched
				if s.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex ++
				newEntriesIndex ++
			}

			// append new logs to s.log
			if newEntriesIndex < len(args.Entries) {
				s.debugLog("Appending log entries %v from index %d",
					args.Entries[newEntriesIndex:], logInsertIndex)
				for _, entry := range args.Entries[newEntriesIndex:] {
					s.log = append(s.log, *entry)
				}
			}

			// set new s.commitIndex by args.LeaderCommit
			if args.LeaderCommit > s.commitIndex {
				s.commitIndex = minInt(args.LeaderCommit, int64(len(s.log)) - 1)
				s.debugLog("Setting commitIndex = %d\n", s.commitIndex)
				s.newCommitReadyChan <- struct{}{}
			}
		}
	}
	reply.Term = s.curTerm
	return &reply, nil
}

// electionTimeout returns election timeout duration
func (s *Server) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 &&
		rand.Intn(3) == 0 {
		return time.Duration(electionTime) * time.Millisecond
	} else {
		return time.Duration(electionTime + rand.Intn(electionTime)) * time.Millisecond
	}
}

// runElectionTimer should be invoked whenever a server is waiting to start another election.
// It periodically monitors the state of server and print information before return.
// It should be launched by a seperated goroutine and returns when state changes or term changes
func (s *Server) runElectionTimer() {
	duration := s.electionTimeout()
	s.mu.Lock()
	termStarted := s.curTerm
	s.mu.Unlock()
	s.debugLog("Election timer started for %v ms, term = %d\n", duration, termStarted)

	ticker := time.NewTicker(checkInterval * time.Millisecond)
	defer ticker.Stop()
	// periodically monitors state
	for {
		<- ticker.C
		s.mu.Lock()
		// become leader or dead: exit monitoring
		if s.state != Candidate && s.state != Follower {
			s.debugLog("In election timer state = %d\n", s.state)
			s.mu.Unlock()
			return
		}

		// another server become leader
		if s.curTerm > termStarted {
			s.debugLog("In election timer term changed from %d to %d", termStarted, s.curTerm)
			s.mu.Unlock()
			return
		}

		// timeout and begin another election
		if elapsed := time.Since(s.electionResetEvent); elapsed >= duration {
			s.debugLog("Election timer timeout, start next election\n")
			s.startElection()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// startElection starts an election for server.
// It multi-cast vote requests to peers and counting received votes.
// It will call runElectionTimer() when an election fails
func (s *Server) startElection() {
	s.state = Candidate
	s.curTerm += 1
	s.votedFor = s.sid
	s.electionResetEvent = time.Now()
	savedCurTerm := s.curTerm
	cumRecvVote := 1
	s.debugLog("Become candidate for term %d\n", savedCurTerm)

	for i := 0; i < PeerNum; i ++ {
		go func(i int) {
			s.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := s.lastLogIndexAndTerm()
			s.mu.Unlock()
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead  {
				return
			}
			voteReq := kv.RequestVoteArgs{
				Term: savedCurTerm,
				CandidateId: s.sid,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm: savedLastLogTerm,
			}
			if voteReply, err := s.peerHandlers[i].RequestVote(context.Background(), &voteReq); err == nil {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.debugLog("Received RequestVoteReply %+v\n", voteReply)
				// if other routine found term > s.curTerm, s becomes follower; or s becomes leader
				if s.state != Candidate {
					return
				}

				// if found term > s.curTerm, s becomes follower
				if voteReply.Term > savedCurTerm {
					s.becomeFollower(voteReply.Term)
					return
				} else if voteReply.Term == savedCurTerm {
					// check cumulative votes
					if voteReply.VoteGranted == true {
						cumRecvVote += 1
						if 2 * cumRecvVote > PeerNum + 1 {
							s.debugLog("Won election with %d votes\n", cumRecvVote)
							s.startLeader()
							return
						}
					}
				}
			}
		}(i)
	}

	// if election failed, wait for next election
	go s.runElectionTimer()
}

func (s *Server) becomeFollower(term int64) {
	s.debugLog("Become follower with term-%d\n", term)
	s.state = Follower
	s.curTerm = term
	s.votedFor = -1
	s.electionResetEvent = time.Now()

	go s.runElectionTimer()
}

func (s *Server) startLeader() {
	s.state = Leader
	s.leaderId = s.sid
	s.debugLog("Become leader with term-%d\n", s.curTerm)

	// new leader reset s.nextIndex and s.matchIndex
	for i := 0; i < PeerNum; i ++ {
		s.nextIndex[i] = int64(len(s.log))
		s.matchIndex[i] = -1
	}

	go func() {
		ticker := time.NewTicker(heartbeatInterval * time.Millisecond)
		defer ticker.Stop()
		for {
			s.leaderHeartbeats()
			<- ticker.C

			// monitor self state: return until server is not a leader
			s.mu.Lock()
			if s.state != Leader {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
	}()
}

func (s *Server) leaderHeartbeats() {
	s.mu.Lock()
	savedCurTerm := s.curTerm
	s.mu.Unlock()
	//s.debugLog("Leader sending heartbeats")

	// send heartbeat to peers
	for i := 0; i < PeerNum; i ++ {
		go func(i int) {
			s.mu.Lock()
			ni := s.nextIndex[i]
			prevLogIndex := ni - 1
			prevLogTerm := int64(-1)
			if prevLogIndex >= 0 {
				prevLogTerm = s.log[prevLogIndex].Term
			}
			entries := []*kv.LogEntry{}
			for _, entry := range s.log[ni:] {
				entries = append(entries, &entry)
			}

			appendReq := kv.AppendEntriesArg{
				Term: savedCurTerm,
				LeaderId: s.sid,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: entries,
				LeaderCommit: s.commitIndex,
			}
			s.mu.Unlock()
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead {
				return
			}
			reply, err := s.peerHandlers[i].AppendEntries(context.Background(), &appendReq)
			if err != nil {
				log.Fatal(err)
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			if reply.Term > savedCurTerm {
				s.debugLog("Term-%d out of date in heartbeat\n", savedCurTerm)
				s.becomeFollower(reply.Term)
				return
			}

			if s.state == Leader && savedCurTerm == reply.Term {
				// successfully append log
				if reply.Success {
					s.nextIndex[i] = ni + int64(len(entries))
					s.matchIndex[i] = s.nextIndex[i] - 1
					//s.debugLog("Successfully receive heartbeat reply from server-%d\n", i)

					savedCommitIndex := s.commitIndex
					for j := s.commitIndex + 1; j < int64(len(s.log)); j ++ {
						if s.log[j].Term == s.curTerm {
							matchCnt := 1
							for peer := 0; peer < PeerNum; peer ++ {
								if s.matchIndex[peer] >= j {
									matchCnt ++
								}
							}
							if matchCnt * 2 > PeerNum + 1 {
								s.commitIndex = j
							}
						}
					}
					if s.commitIndex != savedCommitIndex {
						s.debugLog("Leader set new commitIndex = %d\n", s.commitIndex)
						s.newCommitReadyChan <- struct{}{}
					}
				} else {
					s.nextIndex[i] = ni - 1
					s.debugLog("Fail to append log to server-%d with index %d\n", s.sid, i)
				}
			}
		}(i)
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
// log handling

func (s *Server) submit(op kv.OperationType, keyVal kv.KV) kv.KV {
	s.mu.Lock()

	// only Leader submit
	if s.state == Leader {
		chanId := s.curSharedChanId
		s.curSharedChanId = (s.curSharedChanId + 1) % maxSharedChan
		entry := kv.LogEntry{
			Op: op,
			KeyVal: &keyVal,
			Term: s.curTerm,
			ChanId: chanId,
		}
		s.log = append(s.log, entry)
		s.debugLog("Leader submitted log %v", entry)
		s.mu.Unlock()
		returnKV := <- s.sharedChan[chanId]
		return returnKV
	} else {
		s.debugLog("Should not submit to non-Leader")
		s.mu.Unlock()
		return keyVal
	}
}

func (s *Server) commitChanSender() {
	for range s.newCommitReadyChan {
		// find entries to apply
		s.mu.Lock()
		savedTerm := s.curTerm
		savedLastApplied := s.lastApplied
		var entries []kv.LogEntry

		// if found logs committed but not applied
		if s.commitIndex > s.lastApplied {
			entries = s.log[s.lastApplied + 1: s.commitIndex + 1]
			s.lastApplied = s.commitIndex
			s.debugLog("Found committed but not applied log entries: %v", entries)
		}
		s.mu.Unlock()

		// send committed entries to s.commitChan
		for i, entry := range entries {
			s.commitChan <- CommitEntry{
							Op: entry.Op,
							KeyVal: *entry.KeyVal,
							Index: savedLastApplied + int64(i) + 1,
							Term: savedTerm,
							ChanId: entry.ChanId,
			}
		}
		s.debugLog("Entries %v has been appended ", entries)
	}
	s.debugLog("commitChanSender done")
}

func (s *Server) handleCommitEntry() {
	for {
		// blocked waiting for next commit
		entry := <- s.commitChan

		switch entry.Op {
		case kv.OperationType_PUT:
			res, err := s.put(&entry.KeyVal)
			if err != nil {
				log.Fatal(err)
			}
			if s.state == Leader {
				s.sharedChan[entry.ChanId] <- *res.ReturnKV
			}
			s.debugLog("Finish handling Put request for (%s, %s), chanID = %d\n",
				entry.KeyVal.Key, entry.KeyVal.Value, entry.ChanId)

		case kv.OperationType_DELETE:
			res, err := s.delete(&entry.KeyVal)
			if err != nil {
				log.Fatal(err)
			}
			if s.state == Leader {
				s.sharedChan[entry.ChanId] <- *res.ReturnKV
			}
			s.debugLog("Finish handling Delete request for (%s, %s), chanID = %d\n",
				entry.KeyVal.Key, entry.KeyVal.Value, entry.ChanId)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Key-value database operations

func (s *Server) put(in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Put request")
	err := s.cli.Put(in.Key, in.Value)
	if err != nil {
		return &kv.Response{Msg: "Put request error", ReturnKV: in}, err
	}
	return &kv.Response{Msg: "finished Put request", ReturnKV: in}, err
}

func (s *Server) delete(in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Delete request")
	value, err := s.cli.Get(in.Key)
	if err != nil {
		return &kv.Response{Msg: "Delete request error", ReturnKV: in}, err
	}
	in.Value = value
	err = s.cli.Delete(in.Key)
	return &kv.Response{Msg: "finish Delete request", ReturnKV: in}, err
}

func (s *Server) Put(ctx context.Context, in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Put request")
	returnKV := s.submit(kv.OperationType_PUT, *in)
	fmt.Printf("Successfully add (%s, %s)\n", returnKV.Key, returnKV.Value)
	return &kv.Response{Msg: "finished Put request", ReturnKV: &returnKV}, nil
}

func (s *Server) Delete(ctx context.Context, in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Delete request")
	returnKV := s.submit(kv.OperationType_DELETE, *in)
	fmt.Printf("Successfully delete (%s, %s)\n", returnKV.Key, returnKV.Value)
	return &kv.Response{Msg: "finish Delete request", ReturnKV: &returnKV}, nil
}

func (s *Server) Get(ctx context.Context, in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Get request")
	value, err := s.cli.Get(in.Key)
	if err != nil {
		return &kv.Response{Msg: "Get request error", ReturnKV: in}, err
	}
	in.Value = value
	return &kv.Response{Msg: "finish Get request", ReturnKV: in}, err
}
