
from enum import Enum
import asyncio

class Event(Enum):
    ElectionTimeout = 0
    ReplicationTimeout = 1
    VoteRequest = 2
    VoteResponse = 3
    LogRequest = 4
    LogResponse = 5
    Broadcast = 6

class RaftNode:
    def __init__(self, id, term_number, voted_id, role, leader, votes_total, log, commit_length, peers):
        self.id = id
        self.term_number = term_number
        self.voted_id = voted_id
        self.role = role
        self.leader = leader
        self.votes_total = votes_total
        self.log = log
        self.commit_length = commit_length
        self.peers = peers # NOTE: logic in election() assumes self.peers includes self
        # sent_length: Vec<u64>, // []
        # acked_length: Vec<u64>, // []

    async def logic_loop(self):
        input = await self.receive_event()
        match input:
            case Event.ElectionTimeout:
                self.election()
            case Event.ReplicationTimeout:
                self.replicate_log()
            case Event.VoteRequest:
                self.vote_request()
            case Event.VoteResponse:
                self.vote_response()
            case Event.LogRequest():
                self.log_request()
            case Event.LogResponse():
                self.log_response()
            case Event.Broadcast:
                self.broadcast()    
    
    async def election(self):
        """
        Initiates a new leader election. 

        Node:
            (1) Changes to 'candidate',
            (2) Votes for itself, 
            (3) Increments term number, 
            (4) Sends out vote requests to other nodes in the cluster
        """

        if self.role == 'leader':
            return
        
        self.role = 'candidate'
        
        self.id = self.state.id
        self.voted_id = self.id
        self.votes_received = set()
        self.votes_received.add(self.id)

        self.term_number += 1

        # Determine if candidate's last log entry is at least as up-to-date as node's log
        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        
        # Start vote request message 
        msg = {
            'candidate_id': self.id,
            'candidate_term': self.term_number,
            'candidate_loglen': len(self.log),
            'candidate_logterm': last_term
        }
        
        # Send async vote requests to all peers
        for i in range(len(self.peers)):
            if i == self.id:
                continue
            await self.send_vote_request(i, msg)
    
    def replicate_log(self):
        raise NotImplementedError
    
    def vote_request(self):
        raise NotImplementedError
    
    def vote_response(self):
        raise NotImplementedError
    
    def log_request(self):
        raise NotImplementedError
    
    def log_response(self):
        raise NotImplementedError
    
    def broadcast(self):
        raise NotImplementedError
    
    async def receive_event(self):
        raise NotImplementedError