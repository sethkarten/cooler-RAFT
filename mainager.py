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
        reader, writer = asyncio.open_connection('localhost', port)
        return reader, writer

    async def handle_network_message(self):
        while True:
            try:
                data = await self.reader.read(100)
                print('Received:', data.decode())
                response_dict = data.decode()
                self.pipe_piper(response_dict['id'], response_dict)
            except ConnectionRefusedError:
                self.open_connection()
                print("Connection to the server was refused. Opening new")


    async def pipe_layer(self):
        # bunch of ports
        for i in range(self.num_nodes):
            self.node_info.append(self.open_connection(8000+i))
            self.tasks.append(asyncio.create_task(self.handle_network_message))
        await asyncio.gather(self.tasks)
        # wait for msgs from all connections
        # then callback received: -> pipe ;)
        return


if __name__ == '__main__':
    elizabeth = PipeMANager()
    elizabeth.pipe_layer()