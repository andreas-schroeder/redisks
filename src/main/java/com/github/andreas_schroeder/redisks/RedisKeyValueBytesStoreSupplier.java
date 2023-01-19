package com.github.andreas_schroeder.redisks;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import static java.util.Objects.requireNonNull;

public class RedisKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final RedisConnectionProvider connectionProvider;
    private final byte[] keyPrefix;
    private final byte[] keystoreKey;

    public RedisKeyValueBytesStoreSupplier(
            String name, @NotNull RedisConfigSetter redisConfig) {

        requireNonNull(name, "name cannot be null");
        
        this.name = name;
        this.connectionProvider = redisConfig.getConnectionProvider();
        this.keyPrefix = (name + "_v").getBytes(StandardCharsets.UTF_8);
        this.keystoreKey = (name + "_k").getBytes(StandardCharsets.UTF_8);
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        Comparator<Bytes> ordering = Comparator.naturalOrder();
        return new RedisKeyValueBytesStore(name, connectionProvider, keyPrefix, keystoreKey, ordering);
    }

    @Override
    public String metricsScope() {
        return "redis-store";
    }
    
}
