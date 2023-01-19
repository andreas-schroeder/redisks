package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyValueBytesStore.{KeyBytes, ValueBytes}
import com.lambdaworks.redis._
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands
import com.lambdaworks.redis.api.rx.RedisReactiveCommands
import com.lambdaworks.redis.api.sync.RedisCommands
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.{ComputationScheduler, IOScheduler}
import rx.{Observable => JavaObservable}

import java.io.IOException
import java.util
import java.util.concurrent.Executors
import java.util.{Comparator, Objects}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

class RedisKeyValueBytesStore(
                          val name: String,
                          connectionProvider: RedisConnectionProvider,
                          keyPrefix: Array[Byte],
                          keyStoreKeyIn: Array[Byte],
                          keyComparator: Comparator[KeyBytes]
                        ) extends KeyValueStore[KeyBytes, ValueBytes]
  with StrictLogging
  with PendingOperations {

  import RedisKeyValueBytesStore._

  private val keyStoreKeyTemplate: Array[Byte] = new Array[Byte](keyStoreKeyIn.length + 4)
  System.arraycopy(keyStoreKeyIn, 0, keyStoreKeyTemplate, 0, keyStoreKeyIn.length)

  private val keyOrdering: Ordering[KeyBytes] = Ordering.comparatorToOrdering(keyComparator)

  private val threadPool = Executors.newScheduledThreadPool(20)

  private val futuresExecutionContext = ExecutionContext.fromExecutor(threadPool)

  private val retries = new Retries(
    100.millis,
    60.seconds,
    100000,
    ComputationScheduler(),
    threadPool,
    futuresExecutionContext,
    logBackoff,
    logBackoffFailure
  )

  private val scheduler = IOScheduler()

  @volatile private var open = false
  private var ctx: RedisKeyValueStoreContext = _
  private var openIterators = List[RedisKeyValueIterator[KeyBytes, ValueBytes]]()

  override def init(context: ProcessorContext, root: StateStore): Unit = {
    val ctx = createContext(context)
    this.synchronized {
      this.ctx = ctx
      open = true
    }

    if (root != null) context.register(root, (key: Array[Byte], value: Array[Byte]) => {})
  }

  private def iteratorClosed(it: RedisKeyValueIterator[KeyBytes, ValueBytes]): Unit = this.synchronized {
    openIterators = openIterators diff List(it)
  }

  private def validateOpen(): Unit = this.synchronized {
    if (!open) throw new InvalidStateStoreException(s"State store $name is already closed")
  }
  private def keystoreKey: Array[Byte] = keystoreKeyWithPartition(ctx.context.partition)

  private def keystoreKeyWithPartition(partition: Int): ValueBytes = {
    KeyUtils.addPartition(partition, keyStoreKeyTemplate, keyStoreKeyTemplate.length - 4)
    keyStoreKeyTemplate
  }

  override def flush(): Unit = {
    waitOnPendingOperations()
    this.synchronized(ctx).connection.flushCommands()
  }

  override def close(): Unit = {
    open = false
    this.synchronized(openIterators).foreach(_.close())
    waitOnPendingOperations()
    threadPool.shutdownNow()
    this.synchronized(ctx).close()
  }

  override def persistent: Boolean = true

  override def isOpen: Boolean = open

  private def cmd[T](f: RedisReactiveCommands[Array[Byte], ValueBytes] => JavaObservable[T]): Observable[T] =
    toScalaObservable(f(ctx.rxRedis))

  private def logBackoff(ex: Throwable, tryNumber: Int): Unit =
    logger.warn("Attempt {} failed with {}: {}", tryNumber, ex.getClass.getSimpleName, ex.getMessage)

  private def logBackoffFailure(ex: Throwable): Unit =
    logger.warn("Retry with backoff failed, giving up. {}: {}", ex.getClass.getSimpleName, ex.getMessage)

  private def backoff(attempts: Observable[Throwable]): Observable[Any] = retries.backoff(attempts)

  def backoffOrCancelWhen(cancel: => Boolean)(attempts: Observable[Throwable]): Observable[Any] =
    retries.backoffOrCancelWhen(cancel, attempts)

  override def put(key: KeyBytes, value: ValueBytes): Unit = this.synchronized {
    val codec = ctx.keyCodec
    import codec._
    validateOpen()
    Objects.requireNonNull(key, "key cannot be null")
    Objects.requireNonNull(value, "value cannot be null")
    val prefixedRawKey = encodeKeyWithPrefix(key)
    val eventualPut = retries.retryAsync(
      ctx.asyncRedis.evalsha(ctx.putScript,
        ScriptOutputType.STATUS,
        Array(prefixedRawKey, keystoreKey),
        value,
        key.get()))
    addPendingOperations(eventualPut)
  }

  override def putIfAbsent(key: KeyBytes, value: ValueBytes): ValueBytes = this.synchronized {
    validateOpen()
    val codec = ctx.keyCodec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    Objects.requireNonNull(value, "value cannot be null")
    val prefixedRawKey = encodeKeyWithPrefix(key)
    cmd(
      _.evalsha(ctx.putIfAbsentScript,
        ScriptOutputType.VALUE,
        Array(prefixedRawKey, keystoreKey),
        value,
        key.get()))
      .retryWhen(backoff)
      .toBlocking
      .headOrElse(null)
  }

  override def putAll(entries: util.List[KeyValue[KeyBytes, ValueBytes]]): Unit = this.synchronized {
    validateOpen()
    Objects.requireNonNull(entries, "entries cannot be null")
    val codec = ctx.keyCodec
    import codec._
    val map: util.Map[Array[Byte], ValueBytes] = new util.HashMap(entries.size)
    val keys = new ListBuffer[Array[Byte]]
    for (entry <- entries.asScala) {
      val prefixedRawKey = encodeKeyWithPrefix(entry.key)
      map.put(prefixedRawKey, entry.value)
      keys += entry.key.get()
    }
    val obs = cmd(_.mset(map))
      .observeOn(scheduler)
      .subscribeOn(scheduler)
      .retryWhen(backoff)
      .flatMap(_ => cmd(_.sadd(keystoreKey, keys: _*)).retryWhen(backoff))
    addPendingOperations(Future(obs.toBlocking.first)(futuresExecutionContext))
  }

  override def delete(key: KeyBytes): ValueBytes = this.synchronized {
    validateOpen()
    val codec = ctx.keyCodec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    val prefixedRawKey = encodeKeyWithPrefix(key)
    cmd(_.evalsha(ctx.deleteScript, ScriptOutputType.VALUE, Array(prefixedRawKey, keystoreKey), key.get()))
      .retryWhen(backoff)
      .toBlocking
      .headOrElse(null.asInstanceOf[ValueBytes])
  }

  override def get(key: KeyBytes): ValueBytes = this.synchronized {
    validateOpen()
    val codec = ctx.keyCodec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    val rawKey = encodeKeyWithPrefix(key)
    retries.retrySync(ctx.syncRedis.get(rawKey))
  }

  override def range(from: KeyBytes, to: KeyBytes): KeyValueIterator[KeyBytes, ValueBytes] = this.synchronized {
    import keyOrdering._
    all((k: KeyBytes) => from <= k && k <= to)
  }

  override def all: KeyValueIterator[KeyBytes, ValueBytes] = this.synchronized {
    all((k: KeyBytes) => true)
  }

  private def all(predicate: KeyBytes => Boolean): KeyValueIterator[KeyBytes, ValueBytes] = {
    validateOpen()
    val batchSize: Int = 50
    val it: RedisKeyValueIterator[KeyBytes, ValueBytes] = new RedisKeyValueIterator[KeyBytes, ValueBytes](batchSize, name, iteratorClosed)
    openIterators = openIterators :+ it
    streamKeyValues(it, predicate, batchSize)
    it
  }

  private def streamKeyValues(it: RedisKeyValueIterator[KeyBytes, ValueBytes], predicate: KeyBytes => Boolean, batchSize: Int): Unit = {
    val partition = ctx.context.partition
    val partitionKeystoreKey = keystoreKeyWithPartition(partition).clone()

    val streaming = new KeyValueStreaming(it, predicate, batchSize, partition, partitionKeystoreKey, ctx, retries)
    streaming.run()
  }

  override def approximateNumEntries: Long = this.synchronized {
    cmd(_.scard(keystoreKeyWithPartition(ctx.context.partition))).toBlocking.first
  }

  def createContext(context: ProcessorContext): RedisKeyValueStoreContext = {
    val connection = connectionProvider.connect()
    val sync = connection.sync

    val putIfAbsentScript = sync.scriptLoad(PUT_IF_ABSENT_SCRIPT.getBytes())
    val deleteScript = sync.scriptLoad(DELETE_SCRIPT.getBytes())
    val putScript = sync.scriptLoad(PUT_SCRIPT.getBytes())

    new RedisKeyValueStoreContext(RedisKeyCodec.fromKeyPrefix(keyPrefix, () => context.partition), context, connection, putIfAbsentScript, deleteScript, putScript)
  }
  
}

object RedisKeyValueBytesStore extends StrictLogging {

  type KeyBytes = org.apache.kafka.common.utils.Bytes
  type ValueBytes = Array[Byte]

  private def loadScript(name: String): String =
    try {
      Source.fromResource(name).mkString
    } catch {
      case ex: IOException => throw new RuntimeException("Failed to load lua script '" + name + "'", ex)
    }

  private val PUT_IF_ABSENT_SCRIPT: String = loadScript("put_if_absent.lua")
  private val PUT_SCRIPT: String = loadScript("put.lua")
  private val DELETE_SCRIPT: String = loadScript("delete.lua")

  class RedisKeyValueStoreContext(val keyCodec: RedisKeyCodec,
                                  val context: ProcessorContext,
                                  val connection: StatefulRedisConnection[Array[Byte], ValueBytes],
                                  val putIfAbsentScript: String,
                                  val deleteScript: String,
                                  val putScript: String) {

    val rxRedis: RedisReactiveCommands[Array[Byte], ValueBytes] = connection.reactive
    val asyncRedis: RedisAsyncCommands[Array[Byte], ValueBytes] = connection.async
    val syncRedis: RedisCommands[Array[Byte], ValueBytes] = connection.sync

    def close(): Unit = connection.close()
  }
}
