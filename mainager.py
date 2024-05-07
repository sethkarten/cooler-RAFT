import json
import numpy as np
import asyncio
from rpc import RPCManager
from utils import Event, mainager_port, raft_node_base_port, TOTAL_NODES, get_majority

class PipeManager(): 
    def __init__(self, num_nodes):
        self.node_info = []
        self.tasks = []
        self.num_nodes = num_nodes
        self.net = RPCManager(mainager_port, self.msg_callback)
        self.failure_nodes = []
        self.leader = -1

    async def msg_callback(self, flag, msg):
        sender = int(msg['id'])
        if sender in self.failure_nodes:
            return False
        receiver = int(msg['destination'])
        if receiver == -2:
            self.leader = sender
            return
        if sender == -1:
            while int(msg['destination']) in self.failure_nodes:
                msg['destination'] = np.random.randint(0, TOTAL_NODES)
                receiver = msg['destination']
        # print(f'Received msg from node {sender}. Forwarding to {receiver}')
        # print(msg)
        # print('receiver', receiver)
        port = raft_node_base_port + receiver
        # pipe to other node
        await self.net.send_individual_message(msg, port)

    async def failure_timer(self):
        while len(self.failure_nodes) < get_majority(self.failure_nodes):
            await asyncio.sleep(20)
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

    async def start_piping(self):
        await self.net.start_server()
        addr = self.net.server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        # bunch of ports
        async with self.net.server:
            self.tasks.append(asyncio.create_task(self.net.server.serve_forever()))
            self.tasks.append(asyncio.create_task(self.failure_timer()))
            await asyncio.gather(*self.tasks)

async def main(num_nodes):
    elizabeth = PipeManager(num_nodes)
    await elizabeth.start_piping()

if __name__ == '__main__':
    num_nodes = TOTAL_NODES
    asyncio.get_event_loop().run_until_complete(main(num_nodes))