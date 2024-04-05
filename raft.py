
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
    def __init__(self, id, term_number, voted_id, role, leader, votes_total, log, commit_length):
        self.id = id
        self.term_number = term_number
        self.voted_id = voted_id
        self.role = role
        self.leader = leader
        self.votes_total = votes_total
        self.log = log
        self.commit_length = commit_length
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
    
    def election(self):
        raise NotImplementedError
    
    def replicate_log(self):
        raise NotImplementedError
    
    def VoteResponse(self):
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