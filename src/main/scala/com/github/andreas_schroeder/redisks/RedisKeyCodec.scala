package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyValueBytesStore.KeyBytes

trait RedisKeyCodec {
  
  def prefixKey(rawKey: Array[Byte], partition: Int): Array[Byte]

  /**
   * Convert from Redis format to KeyBytes
   *
   * @param bytes raw bytes from Redis
   * @return key exposed by store as KeyBytes
   */
  def decodeKeyWithPrefix(bytes: Array[Byte]): KeyBytes

  /**
   * Convert from Redis format to tuple with key, 
   * prefixedKey as byte arrays; good to have both
   * for executing commands
   *
   * @param rawKey key exposed by store as KeyBytes
   * @return raw bytes from Redis
   */
  def encodeKeyWithPrefix(rawKey: KeyBytes): Array[Byte]

}

object RedisKeyCodec {

  def wrapKeyBytes: Array[Byte] => KeyBytes = org.apache.kafka.common.utils.Bytes.wrap
  def fromKeyPrefix(keyPrefix: Array[Byte], partition: () => Int): RedisKeyCodec = {

    new RedisKeyCodec() {

      override def prefixKey(rawKey: Array[Byte], partition: Int): Array[Byte] = KeyUtils.prefixKey(rawKey, partition, keyPrefix)

      override def decodeKeyWithPrefix(bytes: Array[Byte]): KeyBytes = {
        val prefixLength = 4 + keyPrefix.length
        val rawKey = new Array[Byte](bytes.length - prefixLength)
        System.arraycopy(bytes, prefixLength, rawKey, 0, rawKey.length)
        wrapKeyBytes(rawKey)
      }

      override def encodeKeyWithPrefix(exposedKey: KeyBytes): Array[Byte] =
        KeyUtils.prefixKey(exposedKey.get(), partition(), keyPrefix)
    }
  }
}
