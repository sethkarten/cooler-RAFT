import json
import numpy as np
import asyncio
from rpc import RPCManager
from utils import Event, mainager_port, raft_node_base_port, TOTAL_NODES

class PipeManager(): 
    def __init__(self, num_nodes):
        self.node_info = []
        self.tasks = []
        self.num_nodes = num_nodes
        self.net = RPCManager(mainager_port, self.msg_callback)
        self.failure_nodes = []

    async def msg_callback(self, flag, msg):
        sender = int(msg['id'])
        if sender in self.failure_nodes:
            return False
        receiver = int(msg['destination'])
        if receiver in self.failure_nodes:
            return False
        # print(f'Received msg from node {sender}. Forwarding to {receiver}')
        # print(msg)
        port = raft_node_base_port + receiver
        # pipe to other node
        await self.net.send_individual_message(msg, port)

    async def failure_timer(self):
        await asyncio.sleep(20)
        failure_node = np.random.randint(0,TOTAL_NODES)
        msg = {
            'id': -2,
            'destination': failure_node,
            'flag': Event.Death
        }
        await self.msg_callback(None, msg)
        print(f'Node {failure_node} fails.')
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