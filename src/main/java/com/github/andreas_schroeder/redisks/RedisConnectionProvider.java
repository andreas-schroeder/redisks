package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.masterslave.MasterSlave;
import com.lambdaworks.redis.masterslave.StatefulRedisMasterSlaveConnection;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import static java.util.Objects.requireNonNull;

public interface RedisConnectionProvider {

    StatefulRedisConnection<byte[],byte[]> connect();

    @Contract(pure = true)
    static @NotNull RedisConnectionProvider fromClient(RedisClient client) {
        requireNonNull(client, "client cannot be null");
        return () -> client.connect(ByteArrayCodec.INSTANCE);
    }

    @Contract(pure = true)
    @SuppressWarnings("unused")
    static @NotNull RedisConnectionProvider fromClientAndNodes(RedisClient client, RedisURI... nodes) {
        requireNonNull(client, "client cannot be null");
        return () -> {
            StatefulRedisMasterSlaveConnection<byte[], byte[]> connection =
                    MasterSlave.connect(client, ByteArrayCodec.INSTANCE, Arrays.asList(nodes));
            connection.setReadFrom(ReadFrom.SLAVE_PREFERRED);
            return connection;
        };

    }

}
