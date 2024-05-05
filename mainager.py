import json
import numpy as np
import asyncio
from utils import mainager_port, raft_node_base_port

class PipeManager(): 
    def __init__(self, num_nodes):
        self.node_info = []
        self.tasks = []
        self.num_nodes = num_nodes

    async def pipe_piper(self, id, msg):
        writer = self.node_info[id][1]
        writer.write(msg)
        await writer.drain()

    async def open_connection(self, port):
        # Wait for 30 seconds, then raise TimeoutError
        for i in range(10):
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                return reader, writer
            except (ConnectionRefusedError, TypeError):
                await asyncio.sleep(1)
                if i == 9:
                    raise

    async def handle_network_message(self, reader, writer):
        data = await reader.read(1000)
        # # print('Received:', data.decode())
        # response_dict = json.loads(data.decode())
        # data_buffer = ''
        # while True:
        #     chunk = await reader.read(100)  # Read chunks of the message
        #     if not chunk:
        #         break  # No more data, stop reading
        #     data_buffer += chunk

        # msg = data_buffer.decode()
        response_dict = json.loads(data)
        print(response_dict)
        print(type(response_dict))
        sender = response_dict['candidate_id']
        print(sender, flush=True)
        receiver = response_dict['destination']
        print(f'Received msg from node {sender}. Forwarding to {receiver}')
        # print(response_dict)
        port = raft_node_base_port+response_dict['destination']
        _, writer = await self.open_connection(port)
        # serialized_msg = json.dumps(data_buffer).encode('utf-8')
        writer.write(data)
        await writer.drain()
        # pipe to other node


    async def pipe_layer(self):
        server = await asyncio.start_server(self.handle_network_message, '127.0.0.1', mainager_port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        # bunch of ports
        async with server:
            self.tasks.append(asyncio.create_task(server.serve_forever()))
            # these might not be necessary anymore
            for i in range(self.num_nodes):
                reader, writer = await self.open_connection(raft_node_base_port+i)
                self.node_info.append((reader, writer))
                self.tasks.append(asyncio.create_task(self.handle_network_message(reader, writer)))
            await asyncio.gather(*self.tasks)
        # wait for msgs from all connections
        # then callback received: -> pipe ;)
        return

async def main(num_nodes):
    elizabeth = PipeManager(num_nodes)
    await elizabeth.pipe_layer()

if __name__ == '__main__':
    num_nodes = 2
    asyncio.get_event_loop().run_until_complete(main(num_nodes))