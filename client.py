import asyncio
from rpc import RPCManager
from utils import Event, TOTAL_NODES, DEFAULT_DIR
import numpy as np
import string
import sys
import random

class Client():
    def __init__(self, log_file_path, client_port, interval, mainager_port, num_nodes=TOTAL_NODES):
        self.stdout = open(log_file_path + "client_stdout.txt", "w+")
        sys.stdout = self.stdout
        self.stderr = open(log_file_path + "client_stderr.txt", "w+")
        sys.stderr = self.stderr
        self.leader_id = 0
        self.num_nodes = num_nodes
        print(f'Num nodes {self.num_nodes}')
        self.tasks = []
        self.net = RPCManager(client_port, self.msg_callback, mainager_port)
        self.interval = interval
        asyncio.run(self.start())
        print(f'manager port {mainager_port}')
        print(f'client port {client_port}')
    
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
            await asyncio.sleep(np.random.randint(.9*self.interval,1.1*self.interval))
            data = random.choice(string.ascii_letters)
            sent_successfully = False
            print("Sending", data)
            while not sent_successfully:
                dst = random.randint(0, self.num_nodes)
                print(f'send to {dst}')
                assert dst is not None
                msg = {
                    'id': -1,
                    'destination': dst,
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
