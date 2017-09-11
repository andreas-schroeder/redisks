package com.github.andreas_schroeder.redisks;

import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.ValueScanCursor;
import com.lambdaworks.redis.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Consumer;

public class RedisStoreAdmin {

    private final RedisConnectionProvider provider;

    public RedisStoreAdmin(RedisConnectionProvider provider) {
        this.provider = provider;
    }

    public void clearStore(String name, int partitions) {
        clearStore((name + "v").getBytes(), (name + "k").getBytes(), partitions);
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

class ClearStoreScript {

    private final RedisCommands<byte[], byte[]> commands;

    private final byte[] keyPrefix;

    private final byte[] partitionKeystoreKey;

    private final int partitions;

    private final ScanArgs scanArgs  = ScanArgs.Builder.limit(1000);


    public ClearStoreScript(RedisCommands<byte[], byte[]> commands, byte[] keyPrefix, byte[] partitionKeystoreKey, int partitions) {
        this.commands = commands;
        this.keyPrefix = keyPrefix;
        this.partitionKeystoreKey = partitionKeystoreKey;
        this.partitions = partitions;
    }

    public ClearStoreScript run() {
        for (int i = 0; i < partitions; i++) {
            int partition = i;
            KeyUtils.addPartition(partition, partitionKeystoreKey, partitionKeystoreKey.length - 4);
            forEachKeyPage((keys) -> {
                byte[][] prefixedKeys = new byte[keys.size()][];
                int j = 0;
                for(byte[] key: keys) {
                    prefixedKeys[j++] = KeyUtils.prefixKey(key, partition, keyPrefix);
                }
                commands.del(prefixedKeys);
            });
            commands.del(partitionKeystoreKey);
        }
        return this;
    }

    public void close() {
        commands.flushall();
        commands.close();
    }

    private void forEachKeyPage(Consumer<List<byte[]>> f) {
        ValueScanCursor<byte[]> cursor = null;
        do {
            if (cursor == null) {
                cursor = commands.sscan(partitionKeystoreKey, scanArgs);
            } else {
                cursor = commands.sscan(partitionKeystoreKey, cursor, scanArgs);
            }
            if (!cursor.getValues().isEmpty()) {
                f.accept(cursor.getValues());
            }
        } while (!cursor.isFinished());
    }
}
