import asyncio
import json

class NetworkManager:
    def __init__(self, node_info):
        self.node_info = node_info

    async def start_server(self, host, port, message_handler):
        server = await asyncio.start_server(
            lambda reader, writer: self.handle_connection(reader, writer, message_handler),
            host, port
        )
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader, writer, message_handler):
        data = await reader.read(1024)  # Adjust buffer size as needed
        message = json.loads(data.decode('utf-8'))
        await message_handler(message)
        writer.close()

    async def send_message(self, peer_id, message):
        reader, writer = await self.open_connection(peer_id)
        message = json.dumps(message).encode('utf-8')
        writer.write(message)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def listen(self, port):
        server = await asyncio.start_server(self.handle_connection, '0.0.0.0', port)
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        data = await reader.read(1024)
        message = json.loads(data.decode('utf-8'))
        await self.process_message(message)
        writer.close()

    async def process_message(self, message):
        raise(NotImplementedError)

    def get_peer_address(self, peer_id):
        return self.node_info[peer_id]