import json
import numpy as np
import asyncio
from rpc import RPCManager
from utils import *
import time
from argparse import ArgumentParser
import sys

class PipeManager(): 
    def __init__(self, num_nodes, filepath, mainager_port, raft_node_base_port, interval=32, max_failures=1, latency=0):
        self.stdout = open(filepath + "Pipe_stdout.txt", "w+")
        sys.stdout = self.stdout
        self.stderr = open(filepath + "Pipe_stderr.txt", "w+")
        sys.stderr = self.stderr
        self.node_info = []
        self.tasks = []
        self.num_nodes = num_nodes
        self.net = RPCManager(mainager_port, self.msg_callback, mainager_port)
        self.failure_nodes = []
        self.leader = -1
        self.filepath = filepath
        self.interval = interval
        self.max_failures = max_failures
        self.latency = latency
        self.raft_node_base_port = raft_node_base_port
        print(f'manager port {mainager_port}')
        print(f'raft port {raft_node_base_port}')
        print(f'interval {self.interval}')
        print(f'max_failures {self.max_failures}')
        print(f'latency {self.latency}')


    async def msg_callback(self, flag, msg):
        sender = int(msg['id'])
        if sender in self.failure_nodes:
            return False
        try:
            receiver = int(msg['destination'])
        except TypeError:
            print('setting to 0', msg, file=sys.stderr)
            msg['destination'] = 0
        receiver = int(msg['destination'])
        if receiver == -2:
            self.leader = sender
            return
        if sender == -1:
            while int(msg['destination']) in self.failure_nodes:
                msg['destination'] = np.random.randint(0, self.num_nodes)
                receiver = msg['destination']
        port = self.raft_node_base_port + receiver
        # pipe to other node
        # add network latency
        await asyncio.sleep(self.latency)
        await self.net.send_individual_message(msg, port)

    async def stdout_flush(self):
        while True:
            await asyncio.sleep(5)
            self.stdout.flush()
            self.stderr.flush()

    async def failure_timer(self):
        while len(self.failure_nodes) < min(get_majority(self.failure_nodes), self.max_failures):
            await asyncio.sleep(self.interval)
            if self.leader == -1 or self.leader in self.failure_nodes:
                continue
            # failure_node = np.random.randint(0,TOTAL_NODES)
            failure_node = self.leader
            print(f'Node {failure_node} fails.')
            msg = {
                'id': -2,
                'destination': failure_node,
                'flag': Event.Death
            }
            await self.msg_callback(None, msg)
            self.failure_nodes.append(failure_node)
            commit_to_file("nodefailure", self.filepath + "allinfo.txt", failure_node, "allinfo")

    async def start_piping(self):
        await self.net.start_server()
        addr = self.net.server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        # bunch of ports
        async with self.net.server:
            self.tasks.append(asyncio.create_task(self.net.server.serve_forever()))
            self.tasks.append(asyncio.create_task(self.failure_timer()))
            self.tasks.append(asyncio.create_task(self.stdout_flush()))
            await asyncio.gather(*self.tasks)

async def main(num_nodes, filepath):
    pm = PipeManager(num_nodes, filepath)
    await pm.start_piping()

if __name__ == '__main__':
    parser = ArgumentParser()
    num_nodes = TOTAL_NODES
    parser.add_argument("--filepath", type=str, default=DEFAULT_DIR)
    args = parser.parse_args()
    asyncio.get_event_loop().run_until_complete(main(num_nodes, args.filepath))
