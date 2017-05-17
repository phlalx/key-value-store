# key-value-store

These are my solutions to the labs of MIT's Distributed Systems class (6.824, Spring 2015). (MIT nicely offers their course material to the public).
See [here](http://nil.csail.mit.edu/6.824/2015) for full description.

Lab 1 is an illustration of the map-reduce paradigm. Labs 2-5 implement a key value store with various degree of fault tolerance and performance.
Students are provided with some code to start with and test scripts.

More details (quoted from [here](http://nil.csail.mit.edu/6.824/2015/labs/lab-2.html)):

* Lab 2 uses primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure.
* Lab 3 uses the Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant.
* Lab 4 is a sharded key/value database, where each shard replicates its state using Paxos. This key/value service can perform Put/Get operations in parallel on different shards, allowing it to support applications such as MapReduce that can put a high load on a storage system. Lab 4 also has a replicated configuration service, which tells the shards for what key range they are responsible. It can change the assignment of keys to shards, for example, in response to changing load. Lab 4 has the core of a real-world design for 1000s of servers.
* Lab 5 will add persistence to Lab 4, so that a key/value server can recover from a crash and re-join its replica group.
