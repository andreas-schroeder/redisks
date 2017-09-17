package com.github.andreas_schroeder.redisks

import java.io.IOException
import java.util
import java.util.concurrent.Executors
import java.util.{Comparator, Objects}

import com.lambdaworks.redis._
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands
import com.lambdaworks.redis.api.rx.RedisReactiveCommands
import com.lambdaworks.redis.api.sync.RedisCommands
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.{ComputationScheduler, IOScheduler}
import rx.{Observable => JavaObservable}

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

class RedisKeyValueStore[K, V <: AnyRef](
    val name: String,
    connectionProvider: RedisConnectionProvider,
    keyPrefix: Array[Byte],
    keyStoreKeyIn: Array[Byte],
    keySerde: Serde[K],
    valueSerde: Serde[V],
    keyComparator: Comparator[K]
) extends KeyValueStore[K, V]
    with StrictLogging
    with PendingOperations {
  import RedisKeyValueStore._

  private val keyStoreKeyTemplate: Bytes = new Array[Byte](keyStoreKeyIn.length + 4)
  System.arraycopy(keyStoreKeyIn, 0, keyStoreKeyTemplate, 0, keyStoreKeyIn.length)

  private val keyOrdering: Ordering[K] = Ordering.comparatorToOrdering(keyComparator)

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

  @volatile private var open                       = false
  private var ctx: RedisKeyValueStoreContext[K, V] = _
  private var openIterators                        = List[RedisKeyValueIterator[K, V]]()

  override def init(context: ProcessorContext, root: StateStore): Unit = {
    val ctx = createContext(context)
    this.synchronized {
      this.ctx = ctx
      open = true
    }

    if (root != null) context.register(root, false, (_, _) => ())
  }

  private def iteratorClosed(it: RedisKeyValueIterator[K, V]): Unit = this.synchronized {
    openIterators = openIterators diff List(it)
  }

  private def validateOpen(): Unit = this.synchronized {
    if (!open) throw new InvalidStateStoreException(s"State store $name is already closed")
  }

  private def keystoreKey: Bytes = keystoreKeyWithPartition(ctx.context.partition())

  private def keystoreKeyWithPartition(partition: Int): Bytes = {
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

  private def cmd[T](f: RedisReactiveCommands[Bytes, Bytes] => JavaObservable[T]): Observable[T] =
    toScalaObservable(f(ctx.rxRedis))

  private def logBackoff(ex: Throwable, tryNumber: Int): Unit =
    logger.warn("Attempt {} failed with {}: {}", tryNumber, ex.getClass.getSimpleName, ex.getMessage)

  def logBackoffFailure(ex: Throwable): Unit =
    logger.warn("Retry with backoff failed, giving up. {}: {}", ex.getClass.getSimpleName, ex.getMessage)

  def backoff(attempts: Observable[Throwable]): Observable[Any] = retries.backoff(attempts)

  def backoffOrCancelWhen(cancel: => Boolean)(attempts: Observable[Throwable]): Observable[Any] =
    retries.backoffOrCancelWhen(cancel, attempts)

  override def put(key: K, value: V): Unit = this.synchronized {
    val codec = ctx.codec
    import codec._
    validateOpen()
    Objects.requireNonNull(key, "key cannot be null")
    Objects.requireNonNull(value, "value cannot be null")
    val (vanillaKey, prefixedRawKey) = encodeKeyWithPrefix(key)
    val eventualPut = retries.retryAsync(
      ctx.asyncRedis.evalsha(ctx.putScript,
                             ScriptOutputType.STATUS,
                             Array(prefixedRawKey, keystoreKey),
                             encodeValue(value),
                             vanillaKey))
    addPendingOperations(eventualPut)
  }

  override def putIfAbsent(key: K, value: V): V = this.synchronized {
    validateOpen()
    val codec = ctx.codec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    Objects.requireNonNull(value, "value cannot be null")
    val (vanillaKey, prefixedRawKey) = encodeKeyWithPrefix(key)
    cmd(
      _.evalsha[Bytes](ctx.putIfAbsentScript,
                       ScriptOutputType.VALUE,
                       Array(prefixedRawKey, keystoreKey),
                       encodeValue(value),
                       vanillaKey))
      .retryWhen(backoff)
      .map(decodeValue)
      .toBlocking
      .headOrElse(nullValue)
  }

  override def putAll(entries: util.List[KeyValue[K, V]]): Unit = this.synchronized {
    validateOpen()
    Objects.requireNonNull(entries, "entries cannot be null")
    val codec = ctx.codec
    import codec._
    val map: util.Map[Bytes, Bytes] = new util.HashMap(entries.size)
    val keys                        = new ListBuffer[Bytes]
    for (entry <- entries.asScala) {
      val (vanillaKey, prefixedRawKey) = encodeKeyWithPrefix(entry.key)
      map.put(prefixedRawKey, encodeValue(entry.value))
      keys += vanillaKey
    }
    val obs = cmd(_.mset(map))
      .observeOn(scheduler)
      .subscribeOn(scheduler)
      .retryWhen(backoff)
      .flatMap(_ => cmd(_.sadd(keystoreKey, keys: _*)).retryWhen(backoff))
    addPendingOperations(Future(obs.toBlocking.first)(futuresExecutionContext))
  }

  override def delete(key: K): V = this.synchronized {
    validateOpen()
    val codec = ctx.codec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    val (vanillaKey, prefixedRawKey) = encodeKeyWithPrefix(key)
    cmd(_.evalsha[Bytes](ctx.deleteScript, ScriptOutputType.VALUE, Array(prefixedRawKey, keystoreKey), vanillaKey))
      .retryWhen(backoff)
      .map(decodeValue)
      .toBlocking
      .headOrElse(nullValue)
  }

  override def get(key: K): V = this.synchronized {
    validateOpen()
    val codec = ctx.codec
    import codec._
    Objects.requireNonNull(key, "key cannot be null")
    val rawKey = encodeKeyWithPrefix(key)._2
    retries.retrySync(decodeValue(ctx.syncRedis.get(rawKey)))
  }

  override def range(from: K, to: K): KeyValueIterator[K, V] = this.synchronized {
    import keyOrdering._
    all((k: K) => from <= k && k <= to)
  }

  override def all: KeyValueIterator[K, V] = this.synchronized { all((k: K) => true) }

  private def all(predicate: K => Boolean): KeyValueIterator[K, V] = {
    validateOpen()
    val batchSize: Int                  = 50
    val it: RedisKeyValueIterator[K, V] = new RedisKeyValueIterator[K, V](batchSize, name, iteratorClosed)
    openIterators = openIterators :+ it
    streamKeyValues(it, predicate, batchSize)
    it
  }

  private def streamKeyValues(it: RedisKeyValueIterator[K, V], predicate: (K) => Boolean, batchSize: Int): Unit = {
    val partition            = ctx.context.taskId().partition
    val partitionKeystoreKey = keystoreKeyWithPartition(partition).clone()

    val streaming = new KeyValueStreaming[K, V](it, predicate, batchSize, partition, partitionKeystoreKey, ctx, retries)
    streaming.run()
  }

  override def approximateNumEntries: Long = this.synchronized {
    cmd(_.scard(keystoreKeyWithPartition(ctx.context.taskId().partition))).toBlocking.first
  }

  def createContext(context: ProcessorContext): RedisKeyValueStoreContext[K, V] = {
    val codec      = createCodec(context)
    val connection = connectionProvider.connect()
    val sync       = connection.sync

    val putIfAbsentScript = sync.scriptLoad(PUT_IF_ABSENT_SCRIPT.getBytes())
    val deleteScript      = sync.scriptLoad(DELETE_SCRIPT.getBytes())
    val putScript         = sync.scriptLoad(PUT_SCRIPT.getBytes())

    new RedisKeyValueStoreContext(codec, context, connection, keyPrefix, putIfAbsentScript, deleteScript, putScript)
  }

  def createCodec(context: ProcessorContext): RedisToKafkaCodec[K, V] = {
    val kSerde = if (keySerde == null) context.keySerde.asInstanceOf[Serde[K]] else keySerde
    val vSerde = if (valueSerde == null) context.valueSerde.asInstanceOf[Serde[V]] else valueSerde
    RedisToKafkaCodec.fromSerdes(kSerde, vSerde, name, keyPrefix, () => context.partition())
  }
}

object RedisKeyValueStore extends StrictLogging {

  type Bytes = Array[Byte]

  def loadScript(name: String): String =
    try {
      Source.fromResource(name).mkString
    } catch {
      case ex: IOException => throw new RuntimeException("Failed to load lua script '" + name + "'", ex)
    }

  val PUT_IF_ABSENT_SCRIPT: String = loadScript("put_if_absent.lua")
  val PUT_SCRIPT: String           = loadScript("put.lua")
  val DELETE_SCRIPT: String        = loadScript("delete.lua")

  class RedisKeyValueStoreContext[K, V](val codec: RedisToKafkaCodec[K, V],
                                        val context: ProcessorContext,
                                        val connection: StatefulRedisConnection[Bytes, Bytes],
                                        keyPrefix: Bytes,
                                        val putIfAbsentScript: String,
                                        val deleteScript: String,
                                        val putScript: String) {

    val rxRedis: RedisReactiveCommands[Bytes, Bytes] = connection.reactive
    val asyncRedis: RedisAsyncCommands[Bytes, Bytes] = connection.async
    val syncRedis: RedisCommands[Bytes, Bytes]       = connection.sync

    def close(): Unit = connection.close()
  }
}
