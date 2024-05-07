import asyncio
import json
from utils import Event

class RPCManager:
    def __init__(self, port, msg_callback, mainager_port):
        self.port = port
        self.msg_callback = msg_callback
        self.mainager_port = mainager_port

    async def start_server(self):
        try:
            self.server = await asyncio.start_server(self.handle_network_message, '127.0.0.1', self.port)
        except OSError:
            self.port += 5
            self.start_server()

    async def open_connection(self, port):
        # Wait for 30 seconds, then raise TimeoutError
        for i in range(30):
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                return reader, writer
            except (ConnectionRefusedError, TypeError):
                await asyncio.sleep(1)
    
    async def send_network_message(self, msg):
        assert self.port != -1
        try:
            _, writer = await self.open_connection(self.mainager_port)
            serialized_msg = json.dumps(msg).encode('utf-8')
            writer.write(serialized_msg)
            await writer.drain()
            return True
        except ConnectionRefusedError:
            print("Connection to the server was refused")
            return False
        return False
        
        
    async def send_individual_message(self, msg, port):
        try:
            _, writer = await self.open_connection(port)
            serialized_msg = json.dumps(msg).encode('utf-8')
            writer.write(serialized_msg)
            await writer.drain()
        except ConnectionRefusedError:
            print("Connection to the server was refused")
            return

    async def handle_network_message(self, reader, writer):
        data = await reader.read(1000)
        msg = json.loads(data.decode())
        flag = Event(msg['flag'])
        await self.msg_callback(flag, msg)

    