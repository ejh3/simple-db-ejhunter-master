# simple-db-ejhunter-master
SimpleDB is an implementation of a relational DBMS supporting a subset of SQL syntax and various database operators such as joins (with hash join for optimized equality joins), predicate-based filters, aggregates (AVG, MIN, COUNT, etc.), insertions and deletions. Written as part of a course on database internals.

Features:
- ACID properties upheld via pessimistic concurrency controls and deadlocks resolved with randomized timeouts on transactions. (A simple solution that surprisingly to me is actually used in production systems. Apparently there are problems with the scalability of the wait-for graph)
- Locking is transaction-based, rather than thread or process-based so a single transaction has the capability to run on multiple threads. Two-phase-locking (2PL) is used in gathering a transaction's locks. Locks apply to pages (as opposed to tuples) and both exlusive and shared locking is implemented.
- Transaction performance is optimized with a STEAL (allow writing of dirty pages to disk) NO-FORCE (don't _require_ committed transaction be flushed to disk) buffer management policy.
- A write-ahead undo/redo log and performant crash recovery protocol, inspired by the ARIES algorithm which is used by Microsoft SQL Server, IBM Db2 and other commercial database systems.
- The primary effect of the buffer management and write-ahead log is to make writes to disk fewer, larger, and more sequential than they otherwise would be. It reduces lock contention because locks can be released without having to first write a lot of data. Reads are also reduced by reducing writes, since data is kept in memory longer.

Note:
The features described above were implemented by me, but some of the other parts of this project were given as boilerplate so the class could focus on database internals. The most significant aspect of this is the parser, though I'm taking compilers Fall 2023 so I will have experience writing a parser of my own soon.
