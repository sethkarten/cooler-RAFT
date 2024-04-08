def get_last_log_term(log):
    """
    Returns the last term in log. 
    """
    if log:
        return log[-1].term
    return 0