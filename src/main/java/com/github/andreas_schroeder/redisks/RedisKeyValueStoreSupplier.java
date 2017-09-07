package com.github.andreas_schroeder.redisks;

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
import static java.util.Objects.requireNonNull;

public class RedisKeyValueStoreSupplier<K, V> implements StateStoreSupplier<KeyValueStore> {

    private final String name;
    private final RedisConnectionProvider connectionProvider;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Comparator<K> keyOrdering;
    private final byte[] keyPrefix;
    private final byte[] keystoreKey;
    private final boolean cached;

    public RedisKeyValueStoreSupplier(
            String name,
            RedisConnectionProvider connectionProvider,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Comparator<K> keyComparator,
            byte[] keyPrefix,
            byte[] keystoreKey,
            boolean cached) {

        requireNonNull(name, "name cannot be null");
        requireNonNull(connectionProvider, "connectionProvider cannot be null");
        requireNonNull(keySerde, "keySerde cannot be null");
        requireNonNull(valueSerde, "valueSerde cannot be null");
        requireNonNull(keyComparator, "keyComparator cannot be null");
        requireNonNull(keyPrefix, "keyPrefix cannot be null");

        this.name = name;
        this.connectionProvider = connectionProvider;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyOrdering = keyComparator;
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
        if(cached) {
            return cachedStore();
        }
        return new MeteredKeyValueStore<>(
                new RedisKeyValueStore<>(name, connectionProvider, keyPrefix, keystoreKey, keySerde, valueSerde, keyOrdering),
                "redis-store",
                Time.SYSTEM);
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<K,V> cachedStore() {
        // note that when using caching ordering cannot be considered and is effectively lost.
        Comparator<Bytes> ordering = Comparator.naturalOrder();
        KeyValueStore<Bytes, byte[]> redis = new MeteredKeyValueStore<>(
                new RedisKeyValueStore<>(name, connectionProvider, keyPrefix, keystoreKey, Serdes.Bytes(), Serdes.ByteArray(), ordering),
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
