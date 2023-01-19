package com.github.andreas_schroeder.redisks;

import java.nio.charset.StandardCharsets;

public class RedisStoreAdmin {

    private final RedisConnectionProvider provider;

    public RedisStoreAdmin(RedisConnectionProvider provider) {
        this.provider = provider;
    }

    public void clearStore(String name, int partitions) {
        clearStore(
                (name + "v").getBytes(StandardCharsets.UTF_8), 
                (name + "k").getBytes(StandardCharsets.UTF_8), partitions);
    }

    public void clearStore(byte[] keyPrefix, byte[] keystoreKey, int partitions) {
        ClearStoreScript script = new ClearStoreScript(
                provider.connect().sync(),
                keyPrefix,
                createKeyTemplate(keystoreKey),
                partitions);

        script.run().close();
    }

    private static byte[] createKeyTemplate(byte[] bytes) {
        byte[] keystoreKey = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, keystoreKey, 0, bytes.length);
        return keystoreKey;
    }
}

