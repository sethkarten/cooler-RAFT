import asyncio
from rpc import RPCManager
from utils import client_port, Event
import numpy as np
import string
import random

class Client():
    def __init__(self):
        self.leader_id = 0
        self.num_nodes = 3
        self.tasks = []
        self.net = RPCManager(client_port, self.msg_callback)
        asyncio.run(self.start())
    
    async def start(self):
        await self.net.start_server()

        addr = self.net.server.sockets[0].getsockname()
        print(f'Serving client on {addr}')

        async with self.net.server:
            self.tasks.append(asyncio.create_task(self.net.server.serve_forever()))
            self.tasks.append(asyncio.create_task(self.logic_loop()))
            await asyncio.gather(*self.tasks)
    
    async def logic_loop(self):
        while True:
            await asyncio.sleep(np.random.randint(10,30))
            data = random.choice(string.ascii_letters)
            msg = {
                'destination': np.random.randint(self.num_nodes),
                'flag': Event.Client,
                'data': data
            }
            print("Sending", data)
            await self.net.send_network_message(msg)

    # deprecated
    async def msg_callback(self, flag, msg):
        # self.leader_id = msg['leader']
        self.leader_id = msg.get('leader', 0)
    
if __name__ == '__main__':
    client = Client()
