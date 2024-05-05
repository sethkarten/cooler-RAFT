import json
import numpy as np
import asyncio

class PipeManager(): 
    def __init__(self):
        self.node_info = []
        self.tasks = []
        self.num_nodes = 2

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
        data = await reader.read(100)
        # print('Received:', data.decode())
        response_dict = json.loads(data.decode())
        print(response_dict)
        print(type(response_dict))
        sender = response_dict['candidate_id']
        print(sender, flush=True)
        receiver = response_dict['destination']
        print(f'Received msg from node {sender}. Forwarding to {receiver}')
        # print(response_dict)
        port = 8081+response_dict['destination']
        _, writer = await self.open_connection(port)
        writer.write(data)
        await writer.drain()
        # pipe to other node


    async def pipe_layer(self):
        server = await asyncio.start_server(self.handle_network_message, '127.0.0.1', 8080)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        # bunch of ports
        async with server:
            self.tasks.append(asyncio.create_task(server.serve_forever()))
            # these might not be necessary anymore
            for i in range(self.num_nodes):
                reader, writer = await self.open_connection(8081+i)
                self.node_info.append((reader, writer))
                self.tasks.append(asyncio.create_task(self.handle_network_message(reader, writer)))
            await asyncio.gather(*self.tasks)
        # wait for msgs from all connections
        # then callback received: -> pipe ;)
        return

async def main():
    elizabeth = PipeManager()
    await elizabeth.pipe_layer()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())