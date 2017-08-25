package com.github.andreas_schroeder.redisks;

public class RedisStore {

    public static <K, V> RedisKeyValueStoreBuilder<K, V> keyValueStore(String name) {
        return new RedisKeyValueStoreBuilder<K,V>(name);
    }

}
