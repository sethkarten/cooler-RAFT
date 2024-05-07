from enum import IntEnum
import numpy as np
import os
import time
import csv

# total number of raft nodes
TOTAL_NODES = 3

mainager_port = 8080
client_port = 8081
raft_node_base_port = 8082

class Event(IntEnum):
    ElectionTimeout = 0
    ReplicationTimeout = 1
    VoteRequest = 2
    VoteResponse = 3
    LogRequest = 4
    LogResponse = 5
    Broadcast = 6
    ElectionTimeoutTest = 7
    Debug = 8
    Client = 9
    Death = 10
    Leader = 11


def get_last_log_term(log):
    """
    Returns the last term in log. 
    """
    if log:
        return log[-1]['term']
    return 0

def get_majority(peers):
    """
    Returns the number of nodes required for a majority vote. 
    """
    return int(np.ceil((len(peers) + 1)  / 2))

def count_acks(acked_length, length):
    """
    Count how many peers have acknowledged a log entry at specific index. 
    """
    return sum(1 for ack in acked_length.values() if ack >= length)

def commit_to_file(entry, filepath, id, flag):
    """
    Append the committed entry to a log file. 
    flag denotes the type of information we are writing to the file (i.e. whether to include timestamps).
    """

    print("Writing to txt file...")

    if flag == "log_timestamp":
        file_exists = os.path.exists(filepath)
        with open(filepath, 'a', newline='') as csvfile:
            fieldnames = ['timestamp', 'log_entry']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            current_time = time.time()
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({'timestamp': current_time, 'log_entry': entry['entry']})

    elif flag == "commitlog":
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                f.write("")
        with open(filepath, 'a') as f:
            f.write(json.dumps(entry['entry']) + '\n')
    
    elif flag == "election_timestamp":
        file_exists = os.path.exists(filepath)
        with open(filepath, 'a', newline='') as csvfile:
            fieldnames = ['timestamp', 'event_type', 'node']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            current_time = time.time()
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({'timestamp': current_time, 'event_type': entry, 'node': id})