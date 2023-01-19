package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.RedisClient;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface RedisConfigSetter {

    @Contract(pure = true)
    static @NotNull RedisConfigSetter fromClient(RedisClient client) {
        return () -> client;
    }

    RedisClient getClient();

    default RedisConnectionProvider getConnectionProvider() {
        return RedisConnectionProvider.fromClient(getClient());
    }
}
