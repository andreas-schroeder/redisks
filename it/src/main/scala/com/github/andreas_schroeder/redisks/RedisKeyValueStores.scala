package com.github.andreas_schroeder.redisks

import java.net.ServerSocket
import java.util.Comparator

import com.lambdaworks.redis.RedisClient
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ProcessorContext, TaskId}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

trait RedisKeyValueStores extends MockitoSugar {

  def freePort = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def createContext = {
    val context = mock[ProcessorContext]
    when(context.applicationId).thenReturn("application")
    when(context.topic).thenReturn("topic")
    when(context.partition).thenReturn(1)
    when(context.taskId()).thenReturn(new TaskId(0, 1))
    context
  }

  def createStore(prefix: String, client: RedisClient, context: ProcessorContext) = {
    val store = new RedisKeyValueStore[String, String](
      "store-name",
      client,
      (prefix + "v").getBytes(),
      (prefix + "k").getBytes(),
      Serdes.String(),
      Serdes.String(),
      Comparator.naturalOrder())
    store.init(context, null)
    store
  }
}
