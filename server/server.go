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

	sid 			int64
	state 			State
	curTerm 		int64
	votedFor 		int64
	leaderId		int64
	log 			[]kv.LogEntry
	electionResetEvent time.Time
}

type ServerSetting struct {
	Cli *tikv.RawKVClient
	PeerHandlers []kv.HandlerClient

	LocalIP string
	LocalPort string
	PeerIP []string
	PeerPorts []string
	Sid int64
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
}

func (s *Server) Start() {
	if s.state == Dead {
		go s.becomeFollower(0)
	}
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

///////////////////////////////////////////////////////////////////////////////////////////////
// Election and heartbeats

// RequestVote RPC: handle vote request from candidate
func (s *Server) RequestVote(ctx context.Context, args *kv.RequestVoteArgs) (*kv.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply := kv.RequestVoteReply{}
	s.debugLog("Received vote request from %d, term %d\n", args.CandidateId, args.Term)

	// current term is out of date
	if s.curTerm < args.Term {
		s.debugLog("Current term %d < %d\n", s.curTerm, args.Term)
		s.becomeFollower(args.Term)
	}

	// check whether to vote for
	if s.curTerm == args.Term &&
		(s.votedFor == -1 || s.votedFor == args.CandidateId) {
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
	if s.curTerm == args.Term {
		// restrain candidate to be a follower
		if s.state != Follower {
			s.becomeFollower(args.Term)
		}
		s.electionResetEvent = time.Now()
		reply.Success = true
	} else {
		reply.Success = false
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
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead  {
				return
			}
			voteReq := kv.RequestVoteArgs{Term: savedCurTerm, CandidateId: s.sid}
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
	return
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
		appendReq := kv.AppendEntriesArg{Term: savedCurTerm, LeaderId: s.sid}
		go func(i int) {
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead {
				return
			}
			appendReply, err := s.peerHandlers[i].AppendEntries(context.Background(), &appendReq)
			if err != nil {
				log.Fatal(err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if appendReply.Term > savedCurTerm {
				s.becomeFollower(appendReply.Term)
				return
			}
		}(i)
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Key-value database operations

func (s *Server) syncPut(in *kv.KV) error {
	for i := 0; i < PeerNum; i ++ {
		go func(i int) {
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead {
				return
			}
			_, err := s.peerHandlers[i].Put(context.Background(), in)
			if err != nil {
				log.Fatal("Fail to sync put request in peer database:", err)
			}
		}(i)
	}
	return nil
}

func (s *Server) syncDelete(in *kv.KV) error {
	for i := 0; i < PeerNum; i ++ {
		go func(i int) {
			state := kv.State{}
			if sta, err := s.peerHandlers[i].GetState(context.Background(), &state); err == nil && State(sta.State) == Dead {
				return
			}
			_, err := s.peerHandlers[i].Delete(context.Background(), in)
			if err != nil {
				log.Fatal("Fail to sync delete request in peer database:", err)
			}
		}(i)
	}
	return nil
}

func (s *Server) Put(ctx context.Context, in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Put request")
	if s.state == Leader {
		err := s.syncPut(in)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := s.cli.Put(in.Key, in.Value)
	if err != nil {
		return &kv.Response{Msg: "Put request error", ReturnKV: in}, err
	}
	fmt.Printf("Successfully add (%s, %s)\n", in.Key, in.Value)
	return &kv.Response{Msg: "finished Put request", ReturnKV: in}, err
}

func (s *Server) Delete(ctx context.Context, in *kv.KV) (*kv.Response, error) {
	fmt.Println("Received Delete request")
	value, err := s.cli.Get(in.Key)
	if err != nil {
		return &kv.Response{Msg: "Delete request error", ReturnKV: in}, err
	}
	if s.state == Leader {
		err := s.syncDelete(in)
		if err != nil {
			log.Fatal(err)
		}
	}
	in.Value = value
	err = s.cli.Delete(in.Key)
	fmt.Printf("Successfully delete (%s, %s)\n", in.Key, in.Value)
	return &kv.Response{Msg: "finish Delete request", ReturnKV: in}, err
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

