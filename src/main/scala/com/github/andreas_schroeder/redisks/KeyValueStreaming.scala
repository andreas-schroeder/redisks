package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyValueStore.{Bytes, RedisKeyValueStoreContext}
import com.lambdaworks.redis.api.rx.RedisReactiveCommands
import com.lambdaworks.redis.{ScanArgs, ValueScanCursor}
import org.apache.kafka.streams.KeyValue
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.{Observable, Subject}
import rx.lang.scala.schedulers.ComputationScheduler
import rx.lang.scala.subjects.PublishSubject
import rx.{Observable => JavaObservable}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KeyValueStreaming[K, V](it: RedisKeyValueIterator[K, V],
                              predicate: (K) => Boolean,
                              batchSize: Int,
                              partition: Int,
                              partitionKeystoreKey: Array[Byte],
                              context: RedisKeyValueStoreContext[K, V],
                              retries: Retries) {

  def cmd[T](f: RedisReactiveCommands[Bytes, Bytes] => JavaObservable[T]): Observable[T] =
    toScalaObservable(f(context.rxRedis))

  val backoffOrCancel: Observable[Throwable] => Observable[Any] = retries.backoffOrCancelWhen(it.closed, _)

  val scanArgs: ScanArgs = ScanArgs.Builder.limit(batchSize)

  val cursorSubject: Subject[Option[ValueScanCursor[Bytes]]] = PublishSubject[Option[ValueScanCursor[Bytes]]]()

  val scheduler = ComputationScheduler()

  def run(): Unit = {
    cursorSubject
      .observeOn(scheduler)
      .flatMap(itemsFromCursor)
      .materialize
      .foreach(it.queue.put)

    cursorSubject.onNext(None)
  }

  def itemsFromCursor(maybeCursor: Option[ValueScanCursor[Bytes]]): Observable[KeyValue[K, V]] = maybeCursor match {
    case Some(cursor) if cursor.isFinished || it.closed =>
      cursorSubject.onCompleted()
      Observable.empty
    case maybeLastCursor =>
      nextKeyBatch(maybeLastCursor)
        .observeOn(scheduler)
        .retryWhen(backoffOrCancel)
        .flatMap(collectKeyValues)
  }

  def nextKeyBatch(maybeCursor: Option[ValueScanCursor[Bytes]]): Observable[ValueScanCursor[Bytes]] =
    maybeCursor match {
      case None             => cmd(_.sscan(partitionKeystoreKey, scanArgs))
      case Some(lastCursor) => cmd(_.sscan(partitionKeystoreKey, lastCursor, scanArgs))
    }

  def collectKeyValues(cursor: ValueScanCursor[Bytes]): Observable[KeyValue[K, V]] = {
    import context.codec._

    val rawKeys              = cursor.getValues.asScala
    val (prefixedKeys, keys) = collectKeys(rawKeys)
    if (keys.isEmpty) {
      cursorSubject.onCompleted()
      Observable.empty
    } else {
      cmd(_.mget(prefixedKeys: _*))
        .retryWhen(backoffOrCancel)
        .zipWith(keys)((rawValue, key) => new KeyValue(key, decodeValue(rawValue)))
        .filter(_ => !it.closed)
        .doOnCompleted(cursorSubject.onNext(Some(cursor)))
    }
  }

  def collectKeys(rawKeys: Seq[Bytes]): (mutable.Buffer[Bytes], mutable.Buffer[K]) = {
    import context.codec._
    val prefixedKeys: mutable.Buffer[Bytes] = new mutable.ArrayBuffer(rawKeys.length)
    val keys: mutable.Buffer[K]             = new mutable.ArrayBuffer(rawKeys.length)
    for {
      rawKey <- rawKeys
      parsedKey = decodeKey(rawKey)
      if predicate(parsedKey)
    } {
      prefixedKeys += prefixKey(rawKey, partition)
      keys += parsedKey
    }
    (prefixedKeys, keys)
  }
}
