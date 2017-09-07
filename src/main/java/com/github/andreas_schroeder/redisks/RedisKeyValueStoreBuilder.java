package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.RedisClient;
import org.apache.kafka.common.serialization.Serde;

import java.util.Comparator;
import static java.util.Objects.requireNonNull;

public class RedisKeyValueStoreBuilder<K, V> {
    private final String name;
    private RedisConnectionProvider connectionProvider;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private Comparator<K> keyComparator;
    private boolean cached;
    private byte[] keyPrefix;
    private byte[] keystoreKey;

    public RedisKeyValueStoreBuilder(String name) {
        requireNonNull(name, "name cannot be null");

        this.name = name;
        this.keyPrefix = (name + "v").getBytes();
        this.keystoreKey = (name + "k").getBytes();
    }

    public RedisKeyValueStoreSupplier<K, V> build() {
        return new RedisKeyValueStoreSupplier<>(
                name,
                connectionProvider,
                keySerde,
                valueSerde,
                keyComparator,
                keyPrefix,
                keystoreKey,
                cached);
    }

    public RedisKeyValueStoreBuilder<K, V> withClient(RedisClient redisClient) {
        requireNonNull(redisClient, "redisClient cannot be null");
        this.connectionProvider = RedisConnectionProvider.fromClient(redisClient);
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withConnection(RedisConnectionProvider connectionProvider) {
        requireNonNull(connectionProvider, "connectionProvider cannot be null");
        this.connectionProvider = connectionProvider;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeyPrefix(byte[] prefix) {
        requireNonNull(prefix, "prefix cannot be null");
        this.keyPrefix = prefix;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeys(Serde<K> serde) {
        requireNonNull(serde, "serde cannot be null");
        this.keySerde = serde;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withValues(Serde<V> serde) {
        requireNonNull(serde, "serde cannot be null");
        this.valueSerde = serde;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> cached() {
        this.cached = true;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> cached(boolean cached) {
        this.cached = cached;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeyComparator(Comparator<K> keyComparator) {
        requireNonNull(keyComparator, "keyComparator cannot be null");
        this.keyComparator = keyComparator;
        return this;
    }

}
