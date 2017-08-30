# Redisks

Redis-Based State Stores for Kafka Streams


## Usage

Redisks depends on [lettuce](https://lettuce.io/) for connecting to Redis.
A KeyValueStore (to be used to store the state of a KTable, e.g.), can be created as follows

```java
import com.lambdaworks.redis.RedisClient;
import com.github.andreas_schroeder.redisks.RedisStore;

RedisClient client = ...
StateStoreSupplier<KeyValueStore> store = RedisStore.<String, String>keyValueStore(name)
                .withClient(client)
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .withKeyComparator(Comparator.naturalOrder())
                .build();

```

## Benchmark results

The benchmark can be found [here](/it/src/test/scala/com/github/andreas_schroeder/redisks/RedisKeyValueStoreBenchmark.scala).
The results below were obtained on a MacBook Pro (Retina, 15-inch, Mid 2015),
with a 2,8 GHz Intel Core i7 CPU. Currently, the attainable throughput depends
significantly on the size of the key-value entries stored.


| key/value size  |  put throughput  | get throughput |
|---|---|---|
| 100 bytes   |  2.00 MiB/Sec |  1.36 MiB/Sec |
| 1024 bytes  | 26.43 MiB/Sec | 12.11 MiB/Sec |
| 2048 bytes  | 46.12 MiB/Sec | 21.20 MiB/Sec |
| 4000 bytes  | 71.24 MiB/Sec | 39.27 MiB/Sec |
