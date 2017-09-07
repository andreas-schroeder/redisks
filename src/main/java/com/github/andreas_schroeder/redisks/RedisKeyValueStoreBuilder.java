package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.RedisClient;
import org.apache.kafka.common.serialization.Serde;

import java.util.Comparator;
import java.util.Objects;

public class RedisKeyValueStoreBuilder<K, V> {
    private final String name;
    private RedisClient redisClient;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private Comparator<K> keyOrdering;
    private boolean cached;
    private byte[] keyPrefix;
    private byte[] keystoreKey;

    public RedisKeyValueStoreBuilder(String name) {
        Objects.requireNonNull(name, "name cannot be null");

        this.name = name;
        this.keyPrefix = (name + "v").getBytes();
        this.keystoreKey = (name + "k").getBytes();
    }

    public RedisKeyValueStoreSupplier<K, V> build() {
        return new RedisKeyValueStoreSupplier<>(
                name,
                redisClient,
                keySerde,
                valueSerde,
                keyOrdering,
                keyPrefix,
                keystoreKey,
                cached);
    }


    public RedisKeyValueStoreBuilder<K, V> withClient(RedisClient redisClient) {
        this.redisClient = redisClient;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeys(RedisClient redisClient) {
        this.redisClient = redisClient;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeyPrefix(byte[] prefix) {
        this.keyPrefix = prefix;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withKeys(Serde<K> serde) {
        this.keySerde = serde;
        return this;
    }

    public RedisKeyValueStoreBuilder<K, V> withValues(Serde<V> serde) {
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
        this.keyOrdering = keyComparator;
        return this;
    }

}
