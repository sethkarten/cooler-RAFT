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

    
def replicate(leader_id, peer_id):
    raise NotImplementedError