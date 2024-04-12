import unittest
from raft import RaftNode

class TestRaftNode(unittest.TestCase):
    def test_leader_election(self):
        nodes = [
            RaftNode(id=1, peers=[1,2,3]),
            RaftNode(id=2, peers=[1,2,3]),
            RaftNode(id=3, peers=[1,2,3])
        ]

        for node in nodes:
            node.election() # Start election manually for test

        leaders = [node for node in nodes if node.role == 'leader']
        leader = leaders[0]
        
        # TESTS
        self.assertEqual(len(leaders), 1, "1 leader")
        self.assertTrue(leader.term_number >= 1, "Leader's term >= 1")
        self.assertEqual(leader.leader, leader.id, "Leader's leader should be self")
        for node in nodes:
            self.assertEqual(node.leader, leader.id, "All nodes have same leader")

if __name__ == '__main__':
    unittest.main()
