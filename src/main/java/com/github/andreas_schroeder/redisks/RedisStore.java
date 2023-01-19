package com.github.andreas_schroeder.redisks;

import org.apache.kafka.common.serialization.Serde;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class RedisStore {
    
    @Contract("_, _, _, _ -> new")
    public static <K,V> @NotNull RedisKeyValueStoreBuilder<K,V> builder(String name, RedisConfigSetter redisConfig, Serde<K> keySerde, Serde<V> valueSerde) {
        return new RedisKeyValueStoreBuilder<>(name, redisConfig, keySerde, valueSerde);
    }
    
    public static @NotNull RedisKeyValueBytesStoreSupplier byteStoreSupplier(String name, RedisConfigSetter redisConfig) {
        return new RedisKeyValueBytesStoreSupplier(name, redisConfig);
    }
    
}
