from enum import Enum


class Event(Enum):
    ElectionTimeout = 0
    ReplicationTimeout = 1
    VoteRequest = 2
    VoteResponse = 3
    LogRequest = 4
    LogResponse = 5
    Broadcast = 6
    ElectionTimeoutTest = 0


def get_last_log_term(log):
    """
    Returns the last term in log. 
    """
    if log:
        return log[-1].term
    return 0

def get_majority(peers):
    """
    Returns the number of nodes required for a majority vote. 
    """
    return ((len(peers) + 1) + 1) / 2

def count_acks(acked_length, length):
    """
    Count how many peers have acknowledged a log entry at specific index. 
    """
    return sum(1 for ack in acked_length.values() if ack >= length)