import asyncio
from rpc import RPCManager
from utils import client_port, Event
import numpy as np
import string

class Client():
    def __init__(self):
        self.leader_id = 0
        self.net = RPCManager(self.msg_callback, client_port)
        asyncio.run(self.start())
    
    async def start(self):
        await self.net.start_server()
        async with self.net.server:
            self.tasks.append(asyncio.create_task(self.net.server.serve_forever()))
            self.tasks.append(asyncio.create_task(self.logic_loop()))
            await asyncio.gather(*self.tasks)
    
    async def logic_loop(self):
        while True:
            asyncio.sleep(np.random.randint(10,30))
            data = np.random.choice(string.ascii_letters)
            msg = {
                'destination': self.leader_id,
                'flag': Event.Client,
                'data': data
            }
            self.net.send_network_message(msg)

    async def msg_callback(self, flag, msg):
        self.leader_id = msg['leader']
    
