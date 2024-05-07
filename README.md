Notes:
- Election Timeout needs to be much longer than Replication Timeout or else system breaks. (too many elections -> term is never right + stuff gets written twice in logs, etc.)


Data 
- scaling with number of nodes in a group
- node failures
- packet loss, network latency vs frequency of message loss
- throughput? -> measure correctly!
    - measuring latency not throughput when looking at time
    - keep doubling clients and see how throughput changes (make sure load is high enough)
    - change latency between nodes, nodes in system, failure rates
- need to go deep in evaluation:
    - fine enough granularity
        - collect data every 10ms or 50ms to see the dynamics of the requests
        - not just p50, p90 and p99 is important for latency (variance should shoot up around failure)
- focus on deep dive on smaller set of things (dont worry about reconfiguration)
- failure of nodes
    - replica failure vs primary failure look different
    - wall clock vs instantaneous throughput
    - intermittant failures - random number of packets dropped
    - timeout variable for leader election (aggressive vs slow), affect on failure rate
        - what is the ideal failure rate
        - failure rate, timeout, 
Final blog post:
introduction for specific scenarios that we are looking to evaluate
- 90s papers on unreliable failure detectors
small number of scenarios with high detail on for instance raft doing failure handling
focus on experimental evaluation
To-Do:
1. Write a script to output client logs + Raft committed log and compare. 
2. Evaluate throughput. 
3. Write up. 

timestamp writes,
timestamps of elections,
failure timestamp

file path for each

Change number of clients

number of nodes:    3, 10, 30
network latency:    0, 2, 8
vary election timeout:  1, 10, 40, 100
number of failures: 1, 4, 10, 14 up to max for nodes
timeout between failures:   0, 2, 8, 32
clients: 1, 10, 20