
from enum import Enum
import asyncio
from utils import get_last_log_term, get_majority, replicate

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
        self.peers = peers # NOTE: logic in election() assumes self.peers includes self, also assumed ordered where idx == id
        self.sent_length = {peer_id: 0 for peer_id in self.peers}
        self.ack_length = {peer_id: 0 for peer_id in self.peers}

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
        last_term = get_last_log_term(self.log)
        
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
        """
        When ReplicationTimeout, Leader synchronizes log with Followers. 
        """
        if self.role != 'leader': 
            return

        for peer_id in self.peers:
            if peer_id == self.id: 
                continue
            replicate(self, self.id, peer_id)

    
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
        last_term = get_last_log_term(self.log)
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
    
    def vote_response(self, args):
        """
        Updates Node as it receives a response to its vote request. 
            (1) If Node has a majority of votes via get_majority(peers), then Node ==> Leader. 
            (2) Otherwise, stay Candidate. 
        """
        if (self.role == 'candidate') and (args['voter_term'] == self.term_number) and (args['vote']):
            self.votes_received.add(args['voter_id'])

            if len(self.votes_received) >= get_majority(self.peers): 
                self.role = 'leader'
                self.leader = self.id
                for peer_id in self.peers:
                    if peer_id == self.id: continue
                    self.sent_length[peer_id] = len(self.log)
                    self.ack_length[peer_id] = 0
                self.replicate_log()
        elif args['voter_term'] > self.term_number:
            self.role = 'follower'
            self.term_number = args['voter_term']
            self.voted_id = None
            # TODO -- implement + call self.runner.election_timer_reset() 
    
    def log_request(self):
        raise NotImplementedError
    
    def log_response(self):
        raise NotImplementedError
    
    def broadcast(self, log_entry):
        """
        When a broadcast is triggered,
            (1) Leader appends broadcast message to log and sends to Followers. 
            (2) Otherwise, send message to Leader. 
        """
        if self.role == 'leader':
            log_entry = {
                'term': self.term, 
                'entry': log_entry
            }
            self.log.append(log_entry)
            self.ack_length[self.id] = len(self.log)
            self.replicate_log()
        # else:
            # TODO -- implement + call self.runner.forward_broadcast(log_entry) to send to leader if leader != null, otherwise wait 
    
    async def receive_event(self):
        raise NotImplementedError