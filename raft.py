
from argparse import ArgumentParser
import json
from rpc import RPCManager
from utils import *
import asyncio
from utils import get_last_log_term, get_majority, count_acks, mainager_port, raft_node_base_port
import random
import os

class RaftNode:
    def __init__(self, id, node_info, interval, num_nodes, log_file_path, term_number=0, voted_id=None, role='follower', leader=None, votes_total=0, log=None, commit_length=0):
        self.id = id
        self.num_nodes = num_nodes
        self.peers = {i: raft_node_base_port + i for i in range(num_nodes)}
        print("Peers: ", self.peers) # Includes self.id and is ordered by ID
        self.term_number = term_number
        self.voted_id = voted_id
        self.role = role
        self.leader = leader
        self.votes_total = votes_total
        self.log = log if log is not None else [] # TODO: load from persistent storage
        self.commit_length = commit_length # How many log entries have been committed
        self.sent_length = {peer_id: 0 for peer_id in self.peers} # Len of log that leader believes each follower has
        self.ack_length = {peer_id: 0 for peer_id in self.peers}
        self.reader = None
        self.writer = None
        self.electionTimerCounter = 0
        self.interval = interval
        print("Randomly assigned election timeout", self.interval)
        self.port = node_info[id]
        self.net = RPCManager(self.port, self.event_logic)
        self.start_raft_node()

    def start_raft_node(self):
        asyncio.run(self.logic_loop())

    async def election_timer(self):
        assert self.interval != -1
        while True:
            while self.electionTimerCounter < self.interval:
                await asyncio.sleep(1)
                self.electionTimerCounter += 1
            self.electionTimerCounter = 0
            # await self.event_logic(Event.ElectionTimeoutTest, None)
            await self.event_logic(Event.ElectionTimeout, None)
    
    async def replication_timer(self):
        while True:
            await asyncio.sleep(self.interval+5)
            # await self.event_logic(Event.ElectionTimeoutTest, None)
            # await self.event_logic(Event.ReplicationTimeout, None)

    async def logic_loop(self):
        await self.net.start_server()
        addr = self.net.server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with self.net.server:
            task1 = asyncio.create_task(self.election_timer())
            task2 = asyncio.create_task(self.replication_timer())
            task3 = asyncio.create_task(self.net.server.serve_forever())
            await asyncio.gather(task1, task2, task3)   # <--- beautiful 😭

    async def test_msg(self, msg):
        msg_data = {
            'id': self.id,
            'destination': 1-self.id,   # dual
            'type': f'Hiiiii node {1-self.id}. It me, node {self.id}',
            'flag': Event.Debug
        }
        await self.net.send_network_message(msg_data)

    def print_msg(self, msg):
        print('MSG RECEIVED', msg)

    async def process_client(self, msg):
        # if leader
        if self.leader == self.id:
            data = msg['data']
            self.log.append(data)
        else:
            # forward message to the leader
            msg['destination'] = self.leader
            await self.net.send_network_message(msg)

    async def event_logic(self, input, msg):
        # print('event enum received', input)
        match input:
            case Event.ElectionTimeout:
                await self.election()
            case Event.ReplicationTimeout:
                await self.replicate_log()
            case Event.VoteRequest:
                await self.vote_request(msg)
            case Event.VoteResponse:
                await self.vote_response(msg)
            case Event.LogRequest:
                await self.log_request(msg)
            case Event.LogResponse:
                await self.log_response(msg)
            case Event.Broadcast:
                await self.broadcast(msg)   
            case Event.ElectionTimeoutTest:
                await self.test_msg(msg)  
            case Event.Debug:
                print('debug msg')
                self.print_msg(msg) 
            case Event.Client:
                await self.process_client(msg)
            case _:
                raise ValueError
    
    async def election(self):
        """
        Initiates a new leader election. 

        Node:
            (1) Changes to 'candidate',
            (2) Votes for itself, 
            (3) Increments term number, 
            (4) Sends out vote requests to other nodes in the cluster
        """
        print("STARTING ELECTION")
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
            'id': self.id,
            'term': self.term_number,
            'candidate_loglen': len(self.log),
            'candidate_logterm': last_term,
            'flag': Event.VoteRequest
        }

        # Send async vote requests to all peers
        for peer_id in self.peers:
            if peer_id == self.id:
                continue
            resp['destination'] = peer_id
            await self.net.send_network_message(resp)
    
    async def replicate(self, peer_id):
        # Leader sends log entries after sent_length[follower]
        print("REPLICATING LOG FROM ", peer_id)
        prefix = self.sent_length[peer_id]
        suffix = self.log[prefix:]
        p_term = self.log[prefix-1]['term'] if prefix > 0 else 0
        resp = {
            'id': self.id,
            'term': self.term_number,
            'prefix': prefix,
            'suffix': suffix,
            'prefix_len': p_term,
            'commit': self.commit_length,
            'flag': Event.LogRequest,
            'destination': peer_id
        }

        await self.net.send_network_message(resp)
    
    async def replicate_log(self):
        """
        When ReplicationTimeout, Leader synchronizes log with Followers. 
        """
        if self.role != 'leader': 
            return
        print("REPLICATING LOG")
        for peer_id in self.peers:
            if peer_id == self.id: 
                continue
            
            await self.replicate(peer_id)

    
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
        print("RECEIVING VOTE REQUEST AT ", self.id)
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
            (self.voted_id is None or self.voted_id == args['id']):
            self.voted_id = args['id']
            vote = True
        
        resp = {
            'id': self.id,
            'destination': args['id'],
            'term': self.term_number,
            'vote': vote, 
            'flag': Event.VoteResponse
        }

        await self.net.send_network_message(resp)
    
    async def vote_response(self, args):
        """
        Updates Node as it receives a response to its vote request. 
            (1) If Node has a majority of votes via get_majority(peers), then Node ==> Leader. 
            (2) Otherwise, stay Candidate. 
        """
        print("PROCESSING VOTE RESPONSE AT ", self.id)
        if (self.role == 'candidate') and (args['term'] == self.term_number) and (args['vote']):
            self.votes_received.add(args['id'])

            if len(self.votes_received) >= get_majority(self.peers): 
                self.role = 'leader'
                self.leader = self.id
                print("My leader is", self.leader)
                print("I am a", self.role)
                for peer_id in self.peers:
                    if peer_id == self.id: continue
                    self.sent_length[peer_id] = len(self.log)
                    self.ack_length[peer_id] = 0
                await self.replicate_log()
        elif args['term'] > self.term_number:
            self.role = 'follower'
            self.term_number = args['term']
            self.voted_id = None
            self.electionTimerCounter = 0
    
    async def log_request(self, args):
        """
        When Follower receives a sync msg from Leader, it:
            (1) Checks if log is consistent with log that Leader thinks it has. If not, reject.
            (2) Otherwise, Follower appends suffix log entries to its log. 
            (3) If Leader committed log entries, commit same ones. 
        """ 
        print("RECEIVING A LOG REQUEST / HEARTBEAT")
        if args['term'] > self.term_number:
            self.term_number = args['term']
            self.voted_id = None
            self.electionTimerCounter = 0
        
        # If term from log request matches, acknowledge current leader + become follower. 
        elif args['term'] == self.term_number:
            self.role = 'follower'
            self.leader = args['id']
            print("My leader is ", self.leader)
            print("I am a ", self.role)
            self.electionTimerCounter = 0
        
        # Check if logs are consistent. 
        checklog = False
        if len(self.log) >= args['prefix']:
            if (args['prefix'] == 0) or (self.log[args['prefix']-1]['term'] == args['prefix']):
                checklog  = True

        success = True
        # If logs are consistent, update self log to match leader and acknowledge. 
        if checklog and (args['term'] == self.term_number):
            self.log = self.log[:args['prefix_len']] + args['suffix']
            if self.commit_length < args['commit']:
                self.commit_length = min(args['commit'], len(self.log))
            ack = args['prefix'] + len(args['suffix'])
            success = True

        resp = {
            'id': self.id,
            'destination': args['id'],
            'term': self.term_number,
            'ack': ack,
            'success': success,
            'flag': Event.LogResponse
        }

        await self.net.send_network_message(resp)
    
    async def log_response(self, args):
        """
        When Leader receives a log response from Follower, it:
            (1) If term in response is > current term, change to Follower. 
            (2) Otherwise, if sync was successful, update ack_length and sent_length of Follower.
            (3) If sync was unsuccessful, decrement sent_length and try to replicate log again. 
        """
        print("RESPONDING TO LOG REQUEST")
        if args['term'] > self.term_number:
            # Convert to follower
            self.voted_id = None
            self.term_number = args['term']
            self.role = 'follower'
            self.electionTimerCounter = 0

        elif args['term'] == self.term_number and self.role == 'leader':
            if args['success'] and args['ack'] >= self.ack_length[args['id']]:
                self.sent_length[args['id']] = args['ack']
                self.ack_length[args['id']] = args['ack']
                self.commit_log()

            elif self.sent_length[args['id']] > 0:
                self.sent_length[args['id']] -= 1
                await self.replicate(self, args['id'])

    async def broadcast(self, payload):
        """
        When a broadcast is triggered,
            (1) Leader appends broadcast message to log and sends to Followers. 
            (2) Otherwise, send message to Leader. 
        """
        if self.role == 'leader':
            self.log.append(payload)
            self.ack_length[self.id] = len(self.log)
            await self.replicate_log()

        elif self.leader is not None and self.leader in self.node_info:
            resp = {
                'id': self.id,
                'payload': payload,
                'destination': self.peers[self.leader],
                'flag': Event.Broadcast
            }
            await self.send_message(resp)
    
    def commit_to_file(self, entry):
        # Append the committed entry to a log file
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'w') as f:
                f.write("")
        with open(self.log_file_path, 'a') as f:
            f.write(json.dumps(entry) + '\n')
    
    def commit_log(self):
        """
        If Leader receives majority of acks, commit the log entry. 
        (1) Iterate thru log entries from last committed index.
        (2) If there are new entries to commit, commit to log. 
        """
        print("COMMIT LOG")
        min_acks = (len(self.peers) + 1) // 2
        ready = 0

        for i in range(self.commit_length + 1, len(self.log) + 1):
            if count_acks(self.acked_length, i) >= min_acks:
                ready = i

        if ready > 0 and self.log[ready - 1]['term'] == self.term_number:
            for i in range(self.commit_length, ready):
                self.commit_log(self.log[i]) # TODO: check!
            self.commit_length = ready


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--id", type=int, default=0)
    parser.add_argument("--num_nodes", type=int, default=2)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--filepath", type=str, default='testlog')
    args = parser.parse_args()
    node_info = {}
    node_info[args.id] = raft_node_base_port + args.id

    n = RaftNode(args.id, node_info, random.randint(args.interval-5,args.interval+5), args.num_nodes, args.filepath)
