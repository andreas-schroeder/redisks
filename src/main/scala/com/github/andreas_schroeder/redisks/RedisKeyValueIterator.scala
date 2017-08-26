package com.github.andreas_schroeder.redisks

import java.util.NoSuchElementException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.state.KeyValueIterator
import rx.lang.scala.Notification
import rx.lang.scala.Notification.OnNext

class RedisKeyValueIterator[K,V](bufferSize: Int) extends KeyValueIterator[K,V] {

  type Item = Notification[KeyValue[K,V]]
  val queue: BlockingQueue[Item] = new ArrayBlockingQueue[Item](bufferSize + 1)
  private val closedFlag = new AtomicBoolean()

  private var done: Boolean = false
  private var peeked: Boolean = false
  private var last: KeyValue[K, V] = _

  override def close(): Unit = {
    queue.clear()
    last = null
    closedFlag.set(true)
  }

  def closed: Boolean = closedFlag.get()

  override def peekNextKey: K = {
    if (peeked) {
      last.key
    } else {
      validateMaybeMoreAvailable()
      take() match {
        case Some(kv) =>
          last = kv
          peeked = true
          last.key
        case None =>
          throw new NoSuchElementException()
      }
    }
  }

  private def validateMaybeMoreAvailable(): Unit =
    if (done && queue.isEmpty) throw new NoSuchElementException

  override def hasNext: Boolean = {
    if(done) {
      false
    } else if (peeked) {
      true
    } else {
      take() match {
        case Some(kv) =>
          last = kv
          peeked = true
          true
        case None =>
          false
      }
    }
  }

  override def next: KeyValue[K, V] = {
    if (peeked) {
      peeked = false
      last
    } else {
      validateMaybeMoreAvailable()
      take() match {
        case Some(kv) => kv
        case _        => throw new NoSuchElementException()
      }
    }
  }

  private def take(): Option[KeyValue[K, V]] = queue.take() match {
    case OnNext(kv) => Some(kv)
    case _ =>
      done = true
      None
  }
}
