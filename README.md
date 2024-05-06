NOT IMPLEMENTED:
- If leader fails, load in log from persistent storage.

Notes:
- Election Timeout needs to be much longer than Replication Timeout or else system breaks. (too many elections -> term is never right + stuff gets written twice in logs, etc.)
