import json
import numpy as np
import asyncio
from rpc import RPCManager
from utils import mainager_port, raft_node_base_port

class PipeManager(): 
    def __init__(self, num_nodes):
        self.node_info = []
        self.tasks = []
        self.num_nodes = num_nodes
        self.net = RPCManager(mainager_port, self.msg_callback)

    async def msg_callback(self, flag, msg):
        # sender = response_dict['id']
        receiver = msg['destination']
        # print(f'Received msg from node {sender}. Forwarding to {receiver}')
        # print(msg)
        port = raft_node_base_port+msg['destination']
        # pipe to other node
        await self.net.send_individual_message(msg, port)

    async def start_piping(self):
        await self.net.start_server()
        addr = self.net.server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        # bunch of ports
        async with self.net.server:
            self.tasks.append(asyncio.create_task(self.net.server.serve_forever()))
            await asyncio.gather(*self.tasks)

async def main(num_nodes):
    elizabeth = PipeManager(num_nodes)
    await elizabeth.start_piping()

if __name__ == '__main__':
    num_nodes = 2
    asyncio.get_event_loop().run_until_complete(main(num_nodes))