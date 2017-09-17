package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyValueStore.Bytes
import org.apache.kafka.common.serialization.Serde

trait RedisToKafkaCodec[K, V] {

  val nullValue: V

  def decodeKeyWithPrefix(bytes: Bytes): K

  def decodeKey(bytes: Bytes): K

  def decodeValue(bytes: Bytes): V

  def encodeKeyWithPrefix(key: K): (Bytes, Bytes)

  def prefixKey(rawKey: Bytes, partition: Int): Bytes

  def encodeKey(key: K): Bytes

  def encodeValue(value: V): Bytes

}

object RedisToKafkaCodec {

  def fromSerdes[K, V](keySerde: Serde[K],
                       valueSerde: Serde[V],
                       topic: String,
                       keyPrefix: Bytes,
                       partition: () => Int): RedisToKafkaCodec[K, V] = {
    val keySerializer   = keySerde.serializer
    val valSerializer   = valueSerde.serializer
    val keyDeserializer = keySerde.deserializer
    val valDeserializer = valueSerde.deserializer

    new RedisToKafkaCodec[K, V]() {

      val nullValue: V = null.asInstanceOf[V]

      def decodeKeyWithPrefix(bytes: Array[Byte]): K = {
        val prefixLength = 4 + keyPrefix.length
        val rawKey       = new Array[Byte](bytes.length - prefixLength)
        System.arraycopy(bytes, prefixLength, rawKey, 0, rawKey.length)
        keyDeserializer.deserialize(topic, rawKey)
      }

      def decodeKey(bytes: Array[Byte]): K = keyDeserializer.deserialize(topic, bytes)

      def decodeValue(bytes: Array[Byte]): V = {
        if (bytes == null || bytes.length == 0) {
          nullValue
        } else {
          valDeserializer.deserialize(topic, bytes)
        }
      }

      def encodeKeyWithPrefix(key: K): (Array[Byte], Array[Byte]) = {
        val rawKey      = keySerializer.serialize(topic, key)
        val prefixedKey = prefixKey(rawKey, partition())
        (rawKey, prefixedKey)
      }

      override def prefixKey(rawKey: Bytes, partition: Int): Bytes = KeyUtils.prefixKey(rawKey, partition, keyPrefix)

      def encodeKey(key: K): Array[Byte] = keySerializer.serialize(topic, key)

      def encodeValue(value: V): Array[Byte] = {
        if (value == null) {
          Array.empty[Byte]
        } else {
          valSerializer.serialize(topic, value)
        }
      }
    }
  }
}
