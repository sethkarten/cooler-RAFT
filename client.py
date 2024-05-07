import asyncio
from rpc import RPCManager
from utils import Event, TOTAL_NODES, DEFAULT_DIR
import numpy as np
import string
import sys
import random

class Client():
    def __init__(self, log_file_path, client_port):
        self.stdout = open(log_file_path + "client_stdout.txt", "w+")
        sys.stdout = self.stdout
        self.stderr = open(log_file_path + "client_stderr.txt", "w+")
        sys.stderr = self.stderr
        self.leader_id = 0
        self.num_nodes = TOTAL_NODES
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
            self.tasks.append(asyncio.create_task(self.stdout_flush()))
            await asyncio.gather(*self.tasks)
    
    async def stdout_flush(self):
        while True:
            await asyncio.sleep(5)
            self.stdout.flush()
            self.stderr.flush()

    async def logic_loop(self):
        while True:
            await asyncio.sleep(np.random.randint(10,30))
            data = random.choice(string.ascii_letters)
            sent_successfully = False
            print("Sending", data)
            while not sent_successfully:
                msg = {
                    'id': -1,
                    'destination': np.random.randint(self.num_nodes),
                    'flag': Event.Client,
                    'data': data
                }
                sent_successfully = await self.net.send_network_message(msg)
            print('Sent successfully')

    # deprecated
    async def msg_callback(self, flag, msg):
        # self.leader_id = msg['leader']
        self.leader_id = msg.get('leader', 0)
    
if __name__ == '__main__':
    client = Client(DEFAULT_DIR)
