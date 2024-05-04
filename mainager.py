import numpy as np
import asyncio

class PipeMANager(): 
    def __init__(self):
        self.node_info = []
        self.tasks = []
        self.num_nodes = 2

    async def pipe_piper(self, id, msg):
        writer = self.node_info[id][1]
        writer.write(msg)
        await writer.drain()

    def open_connection(self, port):
        reader, writer = asyncio.open_connection('127.0.0.1', port)
        return reader, writer

    async def handle_network_message(self, reader, writer):
        data = await reader.read(100)
        print('Received:', data.decode())
        response_dict = data.decode()
        print(response_dict)
        # pipe to other node


    async def pipe_layer(self):
        server = await asyncio.start_server(self.handle_network_message, '127.0.0.1', 8080)
        # bunch of ports
        for i in range(self.num_nodes):
            self.node_info.append(self.open_connection(8081+i))
            self.tasks.append(asyncio.create_task(self.handle_network_message))
        await asyncio.gather(self.tasks)
        # wait for msgs from all connections
        # then callback received: -> pipe ;)
        return

async def main():
    elizabeth = PipeMANager()
    await elizabeth.pipe_layer()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())