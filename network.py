import asyncio
import json

class NetworkMANager:
    def __init__(self, node_info):
        self.node_info = node_info

    async def start_server(self, host, port, message_handler):
        raise NotImplementedError

    async def open_connection(self, port):
        # Wait for 30 seconds, then raise TimeoutError
        for i in range(30):
            try:
                reader, writer = asyncio.open_connection('127.0.0.1', port)
                return reader, writer
            except (ConnectionRefusedError, TypeError):
                await asyncio.sleep(1)
    

    