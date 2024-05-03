NOT IMPLEMENTED:
- reset_election_timer() / ElectionTimeout
- ReplicationTimeout

NOTE TO SETH:
- Most RAFT functions are implemented (but not tested).
- Almost every function calls self.network_manager.send_message(peer_id, resp).
    - Inside of resp, there is info that needs to be sent to other nodes, and 'type' indicating what kind of message is being sent. 
    - I do some initial handling of message type in RaftNode (in handle_network_message and map_message_to_event).
    - None of the network stuff is implemented, so we can't fully test any of the RAFT functions. 

- Missing network/RPC functions:
    - self.network_manager.start_server -- in RaftNode init
        - Alternatively, replace with some other way to start servers and create a task queue. 
    - self.network_manager.send_message
        - Needs to open connection to other node + send message in 'resp'
    - self.reset_election_timer() -- would also use asyncio rn, so I waited to implement? 

- Need to test I/O working + calling events properly
    - Sending a toy message of some kind (like a heartbeat)
    - Starting servers
