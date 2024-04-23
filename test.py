import unittest
from raft import RaftNode
from network import NetworkManager
from unittest.mock import patch, AsyncMock

class TestRaftNode(unittest.IsolatedAsyncioTestCase):
    async def test_leader_election(self):
        node_info = {1: ('127.0.0.1', 8001), 2: ('127.0.0.1', 8002), 3: ('127.0.0.1', 8003)}
        nodes = [RaftNode(id=i, node_info=node_info) for i in node_info.keys()]
        print(nodes[1].network_manager.node_info)

        with patch.object(NetworkManager, 'send_message', new_callable=AsyncMock) as mock_send:
            await nodes[0].election() # Start election manually for test
            
            # TEST IF NODE STARTS ELECTION + SENDS VOTE_REQUESTS
            self.assertEqual(nodes[0].role, 'candidate', "Node 0 should be candidate")
            self.assertEqual(nodes[0].term_number, 1, "Node 0 should increment term")
            self.assertTrue(mock_send.called, "Messages for vote_request should send")

            # TEST IF VOTES RECEIVED
            nodes[0].votes_received = {1, 2, 3}  
            if len(nodes[0].votes_received) >= len(nodes) // 2 + 1:
                nodes[0].role = 'leader'
                nodes[0].leader = nodes[0].id
            self.assertEqual(nodes[0].role, 'leader', "Node 0 should be leader")
            self.assertEqual(nodes[0].leader, nodes[0].id, "Node 0 should be leader")

            # TEST IF NODE 0 IS LEADER FOR ALL NODES
            for node in nodes:
                self.assertEqual(node.leader, nodes[0].id, "All nodes should have same leader")

if __name__ == '__main__':
    unittest.main()
