from multiprocessing import Process
from argparse import ArgumentParser
from mainager import PipeManager
import asyncio 
from raft import RaftNode
import random

def start_node(id, node_info):
    asyncio.run(RaftNode(id, node_info, random.randint(5,25)).main_loop())

def start_pipe_manager(num_nodes):
    asyncio.run(main(num_nodes))

async def main(num_nodes):
    elizabeth = PipeManager(num_nodes)
    await elizabeth.pipe_layer()

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

