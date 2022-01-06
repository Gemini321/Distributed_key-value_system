# Distributed_key-value_system

A naive implementation of distributed key-value system based on TiKV

## Features

Features of this work are listed below:

1. Distributed key-value storage
2. Concurrent operating
3. Parts of RAFT consensus algorithm(election, heartbeat, log replication)

## Future works

1. Bugs when server crash
2. Persistence of logs
3. Flexible servers
4. Optimizations: usage of chann and goroutine

## References

* [https://github.com/eliben/raft](https://github.com/eliben/raft): An instructional implementation of RAFT consensus algorithm
* [https://github.com/HomerRong/Distributed_key_value_system](https://github.com/HomerRong/Distributed_key_value_system): Implementation of RAFT on leveldb
* [https://github.com/ProsonulHaque/tikv-go-client-example](https://github.com/ProsonulHaque/tikv-go-client-example): Client example of TiKV
* [https://github.com/tikv/tikv](https://github.com/tikv/tikv): Github of TiKV
