package com.github.andreas_schroeder.redisks;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

public class RedisKeyValueStoreBuilder<K,V> extends KeyValueStoreBuilder<K,V> implements StoreBuilder<KeyValueStore<K,V>> {
    
    
    public RedisKeyValueStoreBuilder(String name, RedisConfigSetter redisConfig, Serde<K> keySerde, Serde<V> valueSerde) {
        super(new RedisKeyValueBytesStoreSupplier(name, redisConfig), keySerde, valueSerde, Time.SYSTEM);
        super.withLoggingDisabled();
    }
    
}
