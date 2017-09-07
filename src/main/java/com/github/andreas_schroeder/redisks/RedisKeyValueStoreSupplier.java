package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.RedisClient;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class RedisKeyValueStoreSupplier<K, V> implements StateStoreSupplier<KeyValueStore> {

    private final String name;
    private final RedisClient redisClient;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Comparator<K> keyOrdering;
    private final byte[] keyPrefix;
    private final byte[] keystoreKey;
    private final boolean cached;

    public RedisKeyValueStoreSupplier(
            String name,
            RedisClient redisClient,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Comparator<K> keyOrdering,
            byte[] keyPrefix,
            byte[] keystoreKey,
            boolean cached) {

        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(redisClient, "redisClient cannot be null");
        Objects.requireNonNull(keySerde, "keySerde cannot be null");
        Objects.requireNonNull(valueSerde, "valueSerde cannot be null");
        Objects.requireNonNull(keyOrdering, "keyOrdering cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");

        this.name = name;
        this.redisClient = redisClient;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyOrdering = keyOrdering;
        this.keyPrefix = keyPrefix;
        this.keystoreKey = keystoreKey;
        this.cached = cached;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<K, V> get() {
        Objects.requireNonNull(redisClient, "redisClient cannot be null");
        if(cached) {
            return cachedStore();
        }
        return new MeteredKeyValueStore<>(
                new RedisKeyValueStore<>(name, redisClient, keyPrefix, keystoreKey, keySerde, valueSerde, keyOrdering),
                "redis-store",
                Time.SYSTEM);
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<K,V> cachedStore() {
        // for future reference - if needed.
        // note that ordering cannot be considered and is effectively lost.
        Comparator<Bytes> ordering = Comparator.naturalOrder();
        KeyValueStore<Bytes, byte[]> redis = new MeteredKeyValueStore<>(
                new RedisKeyValueStore<>(name, redisClient, keyPrefix, keystoreKey, Serdes.Bytes(), Serdes.ByteArray(), ordering),
                "redis-store",
                Time.SYSTEM);
        try {
            Constructor<?> c = Class
                    .forName("org.apache.kafka.streams.state.internals.CachingKeyValueStore")
                    .getDeclaredConstructor(KeyValueStore.class, Serde.class, Serde.class);
            c.setAccessible(true);
            return (KeyValueStore<K,V>) c.newInstance(redis, keySerde, valueSerde);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Fatal exception while trying to instantiate cache", e);
        }
    }

    @Override
    public Map<String, String> logConfig() {
        return Collections.emptyMap();
    }

    @Override
    public boolean loggingEnabled() {
        return false;
    }
}
