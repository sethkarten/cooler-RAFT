import unittest
from raft import RaftNode
from unittest.mock import patch, AsyncMock

class TestRaftNode(unittest.IsolatedAsyncioTestCase):
    async def test_leader_election(self):
        node_info = {1: ('127.0.0.1', 8001), 2: ('127.0.0.1', 8002), 3: ('127.0.0.1', 8003)}

        nodes = [
            RaftNode(id=1, peers=node_info.keys(), node_info=node_info),
            RaftNode(id=2, peers=node_info.keys(), node_info=node_info),
            RaftNode(id=3, peers=node_info.keys(), node_info=node_info)
        ]

        with patch('raft.NetworkManager.send_vote_request', new_callable=AsyncMock):
            await nodes[0].election() # Start election manually for test

            leaders = [node for node in nodes if node.role == 'leader']
            leader = leaders[0]
            
            # TESTS
            self.assertEqual(len(leaders), 1, "1 leader")
            print(len(leaders))
            self.assertTrue(leader.term_number >= 1, "Leader's term >= 1")
            self.assertEqual(leader.leader, leader.id, "Leader's leader should be self")
            for node in nodes:
                self.assertEqual(node.leader, leader.id, "All nodes have same leader")

if __name__ == '__main__':
    unittest.main()
