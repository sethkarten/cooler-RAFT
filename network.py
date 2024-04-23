import asyncio
import json

class NetworkManager:
    def __init__(self, node_info):
        self.node_info = node_info

    async def start_server(self, host, port, message_handler):
        raise(NotImplementedError)

    async def send_message(self, peer_id, message):
        raise(NotImplementedError)