from multiprocessing import Process
import asyncio
import time
import numpy.random as random
import sys
from argparse import ArgumentParser

from mainager import PipeManager
from raft import RaftNode
from client import Client
from utils import raft_node_base_port, TOTAL_NODES, DEFAULT_DIR

def start_node(id, node_info, interval, filepath):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        node = RaftNode(id, node_info, random.randint(0.9*interval, 1.1*interval), TOTAL_NODES, filepath)
        loop.run_until_complete(node.main_loop())
    except Exception as e:
        print(f"Exception in node {id}: {e}")
    finally:
        loop.close()

def start_pipe_manager(num_nodes, filepath, interval, max_failures, latency):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main(num_nodes, filepath, interval, max_failures, latency))
    finally:
        loop.close()

def start_client(filepath):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        client = Client(filepath)
        loop.run_until_complete(client.run())
    finally:
        loop.close()

async def main(num_nodes, filepath, interval, max_failures, latency):
    pm = PipeManager(num_nodes, filepath, interval, max_failures, latency)
    await pm.start_piping()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--num_nodes", type=int, default=3)
    parser.add_argument("--interval", type=int, default=20)
    parser.add_argument("--filepath", type=str, default=DEFAULT_DIR)
    parser.add_argument("--latency", type=int, default=8)
    parser.add_argument("--max_failures", type=int, default=1)
    parser.add_argument("--failure_interval", type=int, default=1)
    args = parser.parse_args()

    manager_process = Process(target=start_pipe_manager, args=(args.num_nodes, args.filepath,  args.failure_interval, args.max_failures, args.latency))
    manager_process.start()

    node_info = {}
    for i in range(args.num_nodes):
        node_info[i] = raft_node_base_port + i

    processes = []
    for i in range(args.num_nodes):
        p = Process(target=start_node, args=(i, node_info, args.interval, args.filepath))
        p.start()
        processes.append(p)

    client_process = Process(target=start_client, args=(args.filepath,))
    client_process.start()
    processes.append(client_process)

    for p in processes:
        p.join()
        time.sleep(1)

    manager_process.join()

