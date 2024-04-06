
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
    
    def get_last_log_term(self):
        """
        Returns the last term in log. 
        """
        if self.log:
            return self.log[-1].term
        return 0

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

        # Determine if candidate's last log entry is at least as up-to-date as self log
        last_term = self.get_last_log_term()
        
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

            await self.send_vote_request(i, msg) # TODO
    
    def replicate_log(self):
        raise NotImplementedError
    
    async def vote_request(self, args):
        """
        When Node receives a voting request from a Requester:
            (1) If term of Requester > Node, update term and change to Follower. 
            (2) Determine whether or not to vote by:
                (a) If Requester term < Node, vote=False.
                (b) If Requester log is less updated than Node, vote=False.
                (c) If Node already voted in this term, vote=False. 
                (d) Otherwise, vote=True. 
        """
        # Check if the request's term is greater than the current term
        if args['candidate_term'] > self.term_number:
            self.role = 'follower'
            self.term_number = args['candidate_term']
            self.voted_id = None
        
        # Determine if requester's log is up-to-date vs self log (using log terms or lengths)
        last_term = self.get_last_log_term()
        if args['candidate_logterm'] > last_term:
            checklog = True
        elif args['candidate_logterm'] == last_term and args['candidate_loglen'] >= len(self.log):
            checklog = True
        else:
            checklog = False
        
        # Decide whether to vote for requester
        vote = False      
        if (args['candidate_term'] == self.term_number) and checklog and \
            (self.voted_id is None or self.voted_id == args['candidate_id']):
            self.voted_id = args['candidate_id']
            vote = True
        
        # Send vote response message
        msg = {
            'voter_id': self.id,
            'voter_term': self.term_number,
            'vote': vote
        }

        await self.send_vote_response(args['candidate_id'], msg) # TODO
    
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