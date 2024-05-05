import unittest
import asyncio
from multiprocessing import Process
from raft import RaftNode
from unittest.mock import patch, AsyncMock
from mainager import PipeManager
import asyncio
import random
from argparse import ArgumentParser

def start_node(id, node_info):
    asyncio.run(RaftNode(id, node_info, random.randint(5,25)).main_loop())

def start_pipe_manager(num_nodes):
    asyncio.run(main(num_nodes))

async def main(num_nodes):
    elizabeth = PipeManager(num_nodes)
    await elizabeth.pipe_layer()

class TestRaft(unittest.IsolatedAsyncioTestCase):
    # async def test_leader_election(self):
    #     await self.nodes[0].start_election()
    #     await asyncio.sleep(1) 
    #     self.assertEqual(self.nodes[0].get_role(), 'leader', "Node 0 should be leader after election")
    #     self.assertEqual(self.nodes[0].get_leader(), self.nodes[0].id, "Node 0 should be its own leader")

        # node_info = {1: ('127.0.0.1', 8081), 2: ('127.0.0.1', 8082)}
        # network_manager = PipeManager()
        # await network_manager.pipe_layer()
        # nodes = [RaftNode(id=i, node_info=node_info) for i in node_info.keys()]
        # for node in nodes:
        #     node.start_raft_node(network_manager)

        # with patch.object(NetworkMANager, 'send_message', new_callable=AsyncMock) as mock_send:
        #     await nodes[0].election() # start election manually
            
        #     # TEST IF NODE STARTS ELECTION + SENDS VOTE_REQUESTS
        #     self.assertEqual(nodes[0].role, 'candidate', "Node 0 should be candidate")
        #     self.assertEqual(nodes[0].term_number, 1, "Node 0 should increment term")
        #     self.assertTrue(mock_send.called, "Messages for vote_request should send")

        #     # TEST IF VOTES RECEIVED
        #     nodes[0].votes_received = {1, 2, 3}  
        #     if len(nodes[0].votes_received) >= len(nodes) // 2 + 1:
        #         nodes[0].role = 'leader'
        #         nodes[0].leader = nodes[0].id
        #     self.assertEqual(nodes[0].role, 'leader', "Node 0 should be leader")
        #     self.assertEqual(nodes[0].leader, nodes[0].id, "Node 0 should be leader")

        #     # TEST IF NODE 0 IS LEADER FOR ALL NODES
        #     for node in nodes:
        #         self.assertEqual(node.leader, nodes[0].id, "All nodes should have same leader")

    async def test_leader_election(self):
        await self.nodes[0].start_election()
        await asyncio.sleep(1) 
        self.assertEqual(self.nodes[0].get_role(), 'leader', "Node 0 should be leader after election")
        self.assertEqual(self.nodes[0].get_leader(), self.nodes[0].id, "Node 0 should be its own leader")

        # node_info = {1: ('127.0.0.1', 8081), 2: ('127.0.0.1', 8082)}
        # network_manager = PipeManager()
        # await network_manager.pipe_layer()
        # nodes = [RaftNode(id=i, node_info=node_info) for i in node_info.keys()]
        # for node in nodes:
        #     node.start_raft_node(network_manager)

        # with patch.object(NetworkMANager, 'send_message', new_callable=AsyncMock) as mock_send:
        #     await nodes[0].election() # start election manually
            
        #     # TEST IF NODE STARTS ELECTION + SENDS VOTE_REQUESTS
        #     self.assertEqual(nodes[0].role, 'candidate', "Node 0 should be candidate")
        #     self.assertEqual(nodes[0].term_number, 1, "Node 0 should increment term")
        #     self.assertTrue(mock_send.called, "Messages for vote_request should send")

        #     # TEST IF VOTES RECEIVED
        #     nodes[0].votes_received = {1, 2, 3}  
        #     if len(nodes[0].votes_received) >= len(nodes) // 2 + 1:
        #         nodes[0].role = 'leader'
        #         nodes[0].leader = nodes[0].id
        #     self.assertEqual(nodes[0].role, 'leader', "Node 0 should be leader")
        #     self.assertEqual(nodes[0].leader, nodes[0].id, "Node 0 should be leader")

        #     # TEST IF NODE 0 IS LEADER FOR ALL NODES
        #     for node in nodes:
        #         self.assertEqual(node.leader, nodes[0].id, "All nodes should have same leader")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--num_nodes", type=int, default=2)
    args = parser.parse_args()

    manager_process = Process(target=start_pipe_manager, args=(args.num_nodes,))
    manager_process.start()

    node_info = {}
    for i in range(args.num_nodes):
        node_info[i] = 8081 + i

    processes = []
    for i in range(args.num_nodes):
        p = Process(target=start_node, args=(i, node_info,)) 
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
        asyncio.sleep(1)
    
    manager_process.join()

    unittest.main()
