import asyncio
import json

class NetworkManager:
    def __init__(self, node_info):
        self.node_info = node_info

    async def open_connection(self, peer_id):
        host, port = self.node_info[peer_id]
        return await asyncio.open_connection(host, port)

    async def send_vote_request(self, peer_id, data):
        reader, writer = await self.open_connection(peer_id)
        message = json.dumps(data).encode('utf-8')
        writer.write(message)
        await writer.drain()

        writer.close()
        await writer.wait_closed()

    def get_peer_address(self, peer_id):
        return self.node_info[peer_id]