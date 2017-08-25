package com.github.andreas_schroeder.redisks

import org.apache.kafka.common.serialization.Serde

trait RedisToKafkaCodec[K,V] {

  def decodeKey(bytes: Array[Byte], partition: Int, prefix: Array[Byte]): K

  def decodeKey(bytes: Array[Byte]): K

  def decodeValue(bytes: Array[Byte]): V

  def encodeKey(key: K, partition: Int, prefix: Array[Byte]): (Array[Byte], Array[Byte])

  def encodeKey(key: K): Array[Byte]

  def encodeValue(value: V): Array[Byte]

}

object RedisToKafkaCodec {

  def fromSerdes[K, V](keySerde: Serde[K], valueSerde: Serde[V], topic: String): RedisToKafkaCodec[K, V] = {
    val keySerializer = keySerde.serializer
    val valSerializer = valueSerde.serializer
    val keyDeserializer = keySerde.deserializer
    val valDeserializer = valueSerde.deserializer

    new RedisToKafkaCodec[K, V]() {

      def decodeKey(bytes: Array[Byte], partition: Int, prefix: Array[Byte]): K = {
        val prefixLength = 4 + prefix.length
        val rawKey = new Array[Byte](bytes.length - prefixLength)
        System.arraycopy(bytes, prefixLength, rawKey, 0, rawKey.length)
        keyDeserializer.deserialize(topic, rawKey)
      }

      def decodeKey(bytes: Array[Byte]): K =  keyDeserializer.deserialize(topic, bytes)

      def decodeValue(bytes: Array[Byte]): V =  valDeserializer.deserialize(topic, bytes)

      def encodeKey(key: K, partition: Int, prefix: Array[Byte]): (Array[Byte], Array[Byte]) = {
        val rawKey = keySerializer.serialize(topic, key)
        val prefixedKey = KeyUtils.prefixKey(rawKey, partition, prefix)
        (rawKey, prefixedKey)
      }

      def encodeKey(key: K): Array[Byte] = keySerializer.serialize(topic, key)

      def encodeValue(value: V): Array[Byte] = valSerializer.serialize(topic, value)
    }
  }
}
