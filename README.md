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