# Redisks

[![Build Status](https://travis-ci.org/andreas-schroeder/redisks.svg?branch=master)](https://travis-ci.org/andreas-schroeder/redisks)
[![Download](https://api.bintray.com/packages/and-schroeder/maven/redisks/images/download.svg) ](https://bintray.com/and-schroeder/maven/redisks/_latestVersion)

Redis-Based State Stores for Kafka Streams

## Why

Kafka Streams provides in-memory state stores and disk-based state stores (based on RocksDB) out of the box. While these
options work great in most scenarios, they both rely on local storage and are not the best fit for
[continoulsy delivered](https://martinfowler.com/bliki/ContinuousDelivery.html) apps on
[immutable servers](https://martinfowler.com/bliki/ImmutableServer.html) (or stateless containers). In those scenarios,
using an external state store (such as Redis) allows to save the application state when replacing the whole machine or
the application container. This can allow smoother delivery, as the state stores do not have to be rebuilt before the
app can start processing records.

## How to use

### Gradle

```groovy
repositories {
   jcenter()
}

dependencies {
    compile group: 'com.github.andreas-schroeder', name: 'redisks_2.12', version: '0.0.0'
}
```

### sbt

```scala

resolvers += Resolver.bintrayRepo("and-schroeder", "maven")

libraryDependencies += "com.github.andreas-schroeder" %% "redisks" % "0.0.0"
```

### Defining a state store supplier

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

Some examples for using the Redis state stores can be found in the
[acceptance tests](it/src/test/scala/com/github/andreas_schroeder/redisks/KafkaStreamsAcceptanceSpec.scala).

## Benchmark results

The benchmark can be found [here](/it/src/main/scala/com/github/andreas_schroeder/redisks/RedisKeyValueStoreBenchmark.scala).
The results below were obtained on a MacBook Pro (Retina, 15-inch, Mid 2015),
with a 2,8 GHz Intel Core i7 CPU. Currently, the attainable throughput depends
significantly on the size of the key-value entries stored.


| key/value size  |  put throughput  | get throughput |
|---|---|---|
| 100 bytes   |  6.04 MiB/Sec |  2.23 MiB/Sec |
| 1024 bytes  | 57.28 MiB/Sec | 19.43 MiB/Sec |
| 2048 bytes  | 94.58 MiB/Sec | 33.13 MiB/Sec |
| 4000 bytes  | 129.53 MiB/Sec | 50.79 MiB/Sec |
