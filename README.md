# Redisks

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

Compile and install in your local maven repository, then reference the package in your favourite build tool.

### Gradle

```groovy

dependencies {
    compile group: 'com.github.andreas-schroeder', name: 'redisks_2.12', version: '0.0.1-SNAPSHOT'
}
```

### sbt

```scala

libraryDependencies += "com.github.andreas-schroeder" %% "redisks" % "0.0.1-SNAPSHOT"
```

### Getting a state store

Redisks depends on [lettuce](https://lettuce.io/) for connecting to Redis.
To create a RedisKeyValueStore subclass `RedisConfigSetter` and make it provide a full configured
`RedisConnectionProvider`; then use it as shown below. The library provides a LocalRedisConfigSetter for plain 
connections to localhost; logging is disabled by default.

```java
import com.github.andreas_schroeder.redisks.RedisStore;
import com.github.andreas_schroeder.redisks.impl.LocalRedisConfigSetter;
import org.apache.kafka.streams.processor.StateStore;

class Example {
    RedisConfigSetter redisConfig = new LocalRedisConfigSetter();
    StateStore<String,String> store = RedisStore
            .builder("redis-store-name", redisConfig, Serde.String(), Serdes.String())
            .enableCaching() // here we have a KeyValueStoreBuilder<String,String> as defined by KafkaStreams
            .build();
}
```

Some examples for providing the Redis state stores to `Materialized` instances can be found in the
[acceptance tests](src/test/scala/com/github/andreas_schroeder/redisks/KafkaStreamsAcceptanceSpec.scala).
