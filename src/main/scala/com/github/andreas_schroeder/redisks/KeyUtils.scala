package com.github.andreas_schroeder.redisks

object KeyUtils {

  def prefixKey(rawKey: Array[Byte], partition: Int, prefix: Array[Byte]) = {
    val bytes = new Array[Byte](prefix.length + 4 + rawKey.length)
    System.arraycopy(prefix, 0, bytes, 0, prefix.length)
    addPartition(partition, bytes, prefix.length)
    System.arraycopy(rawKey, 0, bytes, prefix.length + 4, rawKey.length)
    bytes
  }

  def addPartition(partition: Int, bytes: Array[Byte], position: Int): Unit = {
    var pos = position
    bytes(pos) = (partition >>> 24).toByte
    pos += 1
    bytes(pos) = (partition >>> 16).toByte
    pos += 1
    bytes(pos) = (partition >>> 8).toByte
    pos += 1
    bytes(pos) = partition.toByte
  }

}
