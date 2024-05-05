
from argparse import ArgumentParser
import json
from utils import *
import asyncio
from utils import get_last_log_term, get_majority, count_acks

class RaftNode:
    def __init__(self, id, node_info, interval, term_number=0, voted_id=None, role='follower', leader=None, votes_total=0, log=None, commit_length=0):
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
        self.reader = None
        self.writer = None

        self.interval = interval
        print(self.interval)

        # NETWORK CONFIG
        # self.network_manager = NetworkManager(node_info)
        # self.event_queue = asyncio.Queue() 
        self.port = node_info[id]
        # asyncio.create_task(self.network_manager.start_server('0.0.0.0', self.port, self.handle_network_message)) # TODO
        # init all server magic
        self.start_raft_node()

    def start_raft_node(self):
        asyncio.get_event_loop().run_until_complete(self.logic_loop())

    async def open_connection(self, port):
        # Wait for 30 seconds, then raise TimeoutError
        for i in range(10):
            try:
                self.reader, self.writer = await asyncio.open_connection('127.0.0.1', port)
                return
            except (ConnectionRefusedError, TypeError):
                await asyncio.sleep(1)

    async def receive_network_message(self, reader, writer):
        print('Receiving message at node ', self.id)
        assert self.port != -1
        while True:
            try:
                # data = await reader.read(100)
                data_buffer = ''
                while True:
                    chunk = await reader.read(100)  # Read chunks of the message
                    if not chunk:
                        break  # No more data, stop reading
                    data_buffer += chunk.decode()
                    if '\n' in data_buffer:  # Check if the end-of-message delimiter is in the buffer
                        break

                response_dict = json.loads(data_buffer)
                print('Received:', response_dict)
                # response_dict = json.loads(data.decode())
                await self.event_logic(Event(response_dict['flag']), response_dict)
            except ConnectionRefusedError:
                print("Connection to the server was refused.")
    
    async def send_network_message(self, msg):
        print('Sending message ', msg)
        assert self.port != -1
        try:
            await self.open_connection(8080)
            serialized_msg = json.dumps(msg).encode('utf-8')
            self.writer.write(serialized_msg)
            await self.writer.drain()
        except ConnectionRefusedError:
            print("Connection to the server was refused")
        print('finished sending message')

    async def election_timer(self):
        assert self.interval != -1
        while True:
            await asyncio.sleep(self.interval)
            # await self.event_logic(Event.ElectionTimeoutTest, None)
            await self.event_logic(Event.ElectionTimeout, None)
    
    async def replication_timer(self):
        while True:
            await asyncio.sleep(self.interval+5)
            # await self.event_logic(Event.ElectionTimeoutTest, None)
            # await self.event_logic(Event.ReplicationTimeout, None)

    async def logic_loop(self):
        server = await asyncio.start_server(self.receive_network_message, '127.0.0.1', 8081+self.id)
        
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            task1 = asyncio.create_task(self.election_timer())
            task2 = asyncio.create_task(self.replication_timer())
            task3 = asyncio.create_task(server.serve_forever())
            await asyncio.gather(task1, task2, task3)   # <--- beautiful 😭

    async def test_msg(self, msg):
        msg_data = {
            'candidate_id': self.id,
            'destination': 1-self.id,   # dual
            'type': f'Hiiiii node {1-self.id}. It me, node {self.id}',
            'flag': Event.Debug
        }
        await self.send_network_message(msg_data)

    def print_msg(self, msg):
        print('MSG RECEIVED', msg)

    async def event_logic(self, input, msg):
        # print('event enum received', input)
        match input:
            case Event.ElectionTimeout:
                await self.election()
            case Event.ReplicationTimeout:
                return self.replicate_log()
            case Event.VoteRequest:
                return self.vote_request(msg)
            case Event.VoteResponse:
                return self.vote_response(msg)
            case Event.LogRequest:
                return self.log_request(msg)
            case Event.LogResponse:
                return self.log_response(msg)
            case Event.Broadcast:
                return self.broadcast(msg)   
            case Event.ElectionTimeoutTest:
                await self.test_msg(msg)  
            case Event.Debug:
                print('debug msg')
                self.print_msg(msg) 
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
        print("Starting election...")
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
            'flag': Event.VoteRequest
        }

        # Send async vote requests to all peers
        for peer_id in self.peers:
            if peer_id == self.id:
                continue
            resp['destination'] = peer_id
            await self.send_network_message(resp)
    
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

        await self.send_network_message(resp)
    
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
        print("Receiving vote request at Node ", self.id)
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


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--id", type=int, default=0)
    parser.add_argument("--num_nodes", type=int, default=2)
    args = parser.parse_args()
    node_info = {}
    for i in range(args.num_nodes):
        node_info[i] = 8081 + args.id
    n = RaftNode(args.id, node_info)