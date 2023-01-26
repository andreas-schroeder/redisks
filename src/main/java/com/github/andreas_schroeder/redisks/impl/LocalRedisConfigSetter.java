package com.github.andreas_schroeder.redisks.impl;

import com.github.andreas_schroeder.redisks.RedisConfigSetter;
import com.lambdaworks.redis.RedisClient;
import org.jetbrains.annotations.NotNull;

public class LocalRedisConfigSetter implements RedisConfigSetter {

    private static RedisClient client;

    public @NotNull RedisClient getClient() {
        if (client == null) {
            client = RedisClient.create("redis://localhost:6379/");
        }
        return client;
    }

}
