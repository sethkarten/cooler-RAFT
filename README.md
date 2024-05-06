Notes:
- Election Timeout needs to be much longer than Replication Timeout or else system breaks. (too many elections -> term is never right + stuff gets written twice in logs, etc.)

To-Do:
1. Write a script to output client logs + Raft committed log and compare. 
2. Evaluate throughput. 
3. Write up. 
