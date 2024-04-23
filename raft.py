
from enum import Enum
import asyncio
from utils import get_last_log_term, get_majority, count_acks
from network import NetworkManager

class Event(Enum):
    ElectionTimeout = 0
    ReplicationTimeout = 1
    VoteRequest = 2
    VoteResponse = 3
    LogRequest = 4
    LogResponse = 5
    Broadcast = 6

class RaftNode:
    def __init__(self, id, node_info, term_number=0, voted_id=None, role='follower', leader=None, votes_total=0, log=None, commit_length=0):
        self.id = id
        self.peers = node_info.keys() # Includes self.id and is ordered by ID
        self.term_number = term_number
        self.voted_id = voted_id
        self.role = role
        self.leader = leader
        self.votes_total = votes_total
        self.log = log if log is not None else []
        self.commit_length = commit_length # How many log entries have been committed
        self.sent_length = {peer_id: 0 for peer_id in self.peers} # Len of log that leader believes each follower has
        self.ack_length = {peer_id: 0 for peer_id in self.peers}

        # NETWORK CONFIG
        self.network_manager = NetworkManager(node_info)
        self.event_queue = asyncio.Queue() 
        self.port = node_info[self.id][1]
        asyncio.create_task(self.network_manager.start_server('0.0.0.0', self.port, self.handle_network_message)) # TODO

    async def handle_network_message(self, message):
        event_type = self.map_message_to_event(message)
        await self.event_queue.put(event_type)
    
    def map_message_to_event(self, message):
        if message['type'] == 'vote_request':
            return Event.VoteRequest
        elif message['type'] == 'log_response':
            return Event.LogResponse
        elif message['type'] == 'log_request':
            return Event.LogRequest
        elif message['type'] == 'vote_response':
            return Event.VoteResponse
        elif message['type'] == 'broadcast':
            return Event.Broadcast
        return None

    async def logic_loop(self):
        while True:
            input = await self.receive_event() 
            match input:
                case Event.ElectionTimeout:
                    await self.election()
                case Event.ReplicationTimeout:
                    await self.replicate_log()
                case Event.VoteRequest:
                    await self.vote_request()
                case Event.VoteResponse:
                    await self.vote_response()
                case Event.LogRequest():
                    await self.log_request()
                case Event.LogResponse():
                    await self.log_response()
                case Event.Broadcast:
                    await self.broadcast()    

    async def receive_event(self):
        return await self.event_queue.get()
    
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

        self.voted_id = self.id
        self.votes_received = set()
        self.votes_received.add(self.id)

        self.term_number += 1

        # Determine if candidate's last log entry is at least as up-to-date as self log
        last_term = get_last_log_term(self.log)
        
        # Start vote request message 
        resp = {
            'candidate_id': self.id,
            'term': self.term_number,
            'candidate_loglen': len(self.log),
            'candidate_logterm': last_term,
            'type': 'vote_request'
        }

        # Send async vote requests to all peers
        for peer_id in self.peers:
            if peer_id == self.id:
                continue
            await self.network_manager.send_message(peer_id, resp)
    
    async def replicate(self, peer_id):
        # Leader sends log entries after sent_length[follower]
        prefix = self.sent_length[peer_id]
        suffix = self.log[prefix:]
        p_term = self.log[prefix-1]['term'] if prefix > 0 else 0
        resp = {
            'leader': self.id,
            'term': self.term_number,
            'prefix': prefix,
            'suffix': suffix,
            'prefix_term': p_term,
            'commit': self.commit_length,
            'type': 'log_request'
        }

        await self.network_manager.send_message(peer_id, resp)
    
    def replicate_log(self):
        """
        When ReplicationTimeout, Leader synchronizes log with Followers. 
        """
        if self.role != 'leader': 
            return

        for peer_id in self.peers:
            if peer_id == self.id: 
                continue
            
            self.replicate(peer_id)

    
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
        if args['term'] > self.term_number:
            self.role = 'follower'
            self.term_number = args['term']
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
        if (args['term'] == self.term_number) and checklog and \
            (self.voted_id is None or self.voted_id == args['candidate_id']):
            self.voted_id = args['candidate_id']
            vote = True
        
        resp = {
            'voter_id': self.id,
            'term': self.term_number,
            'vote': vote, 
            'type': 'vote_response'
        }

        await self.network_manager.send_message(args['candidate_id'], resp)
    
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
        elif args['term'] > self.term_number:
            self.role = 'follower'
            self.term_number = args['term']
            self.voted_id = None
            # TODO -- implement + call self.runner.election_timer_reset() 
    
    async def log_request(self, args):
        """
        When Follower receives a sync msg from Leader, it:
            (1) Checks if log is consistent with log that Leader thinks it has. If not, reject.
            (2) Otherwise, Follower appends suffix log entries to its log. 
            (3) If Leader committed log entries, commit same ones. 
        """ 
        if args['term'] > self.term_number:
            self.term_number = args['term']
            self.voted_id = None
            # TODO -- implement + call self.runner.election_timer_reset() -- bc node is out of date.
        
        # If term from log request matches, acknowledge current leader + become follower. 
        elif args['term'] == self.term_number:
            self.role = 'follower'
            self.leader = args['leader']
            # TODO -- implement + call self.runner.election_timer_reset()
        
        # Check if logs are consistent. 
        checklog = False
        if len(self.log) >= args['prefix']:
            if (args['prefix'] == 0) or (self.log[args['prefix']-1]['term'] == args['prefix']):
                checklog  = True

        success = True
        # If logs are consistent, update self log to match leader and acknowledge. 
        if checklog and (args['term'] == self.term_number):
            self.log = self.log[:args['prefix_len']] + args['suffix']
            if self.commit_length < args['commit_length']:
                self.commit_length = min(args['commit_length'], len(self.log))
            ack = args['prefix'] + len(args['suffix'])
            success = True

        resp = {
            'follower': self.id,
            'term': self.term_number,
            'ack': ack,
            'success': success,
            'type': 'log_response'
        }

        await self.network_manager.send_message(args['leader'], resp)
    
    async def log_response(self, args):
        """
        When Leader receives a log response from Follower, it:
            (1) If term in response is > current term, change to Follower. 
            (2) Otherwise, if sync was successful, update ack_length and sent_length of Follower.
            (3) If sync was unsuccessful, decrement sent_length and try to replicate log again. 
        """
        if args['term'] > self.term_number:
            # Convert to follower
            self.voted_id = None
            self.term_number = args['term']
            self.role = 'follower'
            await self.election_timer_reset() # TODO

        elif args['term'] == self.term_number and self.role == 'leader':
            if args['success'] and args['ack'] >= self.ack_length[args['follower']]:
                self.sent_length[args['follower']] = args['ack']
                self.ack_length[args['follower']] = args['ack']
                self.commit_log()

            elif self.sent_length[args['follower']] > 0:
                self.sent_length[args['follower']] -= 1
                await self.replicate(self, args['follower'])

    async def broadcast(self, payload):
        """
        When a broadcast is triggered,
            (1) Leader appends broadcast message to log and sends to Followers. 
            (2) Otherwise, send message to Leader. 
        """
        if self.role == 'leader':
            log_entry = {
                'term': self.term, 
                'entry': payload
            }
            self.log.append(log_entry)
            self.ack_length[self.id] = len(self.log)
            await self.replicate_log()
        elif self.leader is not None and self.leader in self.node_info:
            resp = {
                'payload': payload,
                'type': 'broadcast'
            }
            await self.network_manager.send_message(self.node_info[self.leader], resp)
    
    def commit_log(self):
        """
        If Leader receives majority of acks, commit the log entry. 
        (1) Iterate thru log entries from last committed index.
        (2) If there are new entries to commit, commit to log. 
        """

        min_acks = (len(self.peers) + 1) // 2
        ready = 0

        for i in range(self.commit_length + 1, len(self.log) + 1):
            if count_acks(self.acked_length, i) >= min_acks:
                ready = i

        if ready > 0 and self.log[ready - 1]['term'] == self.term_number:
            for i in range(self.commit_length, ready):
                # TODO: self.deliver_message(self.log[i]['payload']) ??
                print("bleh")
            self.commit_length = ready
