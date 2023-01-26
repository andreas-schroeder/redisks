package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyCodec.wrapKeyBytes
import com.github.andreas_schroeder.redisks.RedisKeyValueBytesStore.{KeyBytes, RedisKeyValueStoreContext, ValueBytes}
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

class KeyValueStreaming(it: RedisKeyValueIterator[KeyBytes, ValueBytes],
                        predicate: KeyBytes => Boolean,
                        batchSize: Int,
                        partition: Int,
                        partitionKeystoreKey: Array[Byte],
                        context: RedisKeyValueStoreContext,
                        retries: Retries) {

  def cmd[T](f: RedisReactiveCommands[Array[Byte], Array[Byte]] => JavaObservable[T]): Observable[T] =
    toScalaObservable(f(context.rxRedis))

  private val backoffOrCancel: Observable[Throwable] => Observable[Any] = retries.backoffOrCancelWhen(it.closed, _)

  val scanArgs: ScanArgs = ScanArgs.Builder.limit(batchSize)

  private val cursorSubject: Subject[Option[ValueScanCursor[Array[Byte]]]] = PublishSubject[Option[ValueScanCursor[Array[Byte]]]]()

  val scheduler: ComputationScheduler = ComputationScheduler()

  def run(): Unit = {
    cursorSubject
      .observeOn(scheduler)
      .flatMap(itemsFromCursor)
      .materialize
      .foreach(it.queue.put)

    cursorSubject.onNext(None)
  }

  private def itemsFromCursor(maybeCursor: Option[ValueScanCursor[Array[Byte]]]): Observable[KeyValue[KeyBytes, ValueBytes]] = maybeCursor match {
    case Some(cursor) if cursor.isFinished || it.closed =>
      cursorSubject.onCompleted()
      Observable.empty
    case maybeLastCursor =>
      nextKeyBatch(maybeLastCursor)
        .observeOn(scheduler)
        .retryWhen(backoffOrCancel)
        .flatMap(collectKeyValues)
  }

  private def nextKeyBatch(maybeCursor: Option[ValueScanCursor[ValueBytes]]): Observable[ValueScanCursor[ValueBytes]] =
    maybeCursor match {
      case None             => cmd(_.sscan(partitionKeystoreKey, scanArgs))
      case Some(lastCursor) => cmd(_.sscan(partitionKeystoreKey, lastCursor, scanArgs))
    }

  private def collectKeyValues(cursor: ValueScanCursor[ValueBytes]): Observable[KeyValue[KeyBytes, ValueBytes]] = {

    val rawKeys              = cursor.getValues.asScala
    val (prefixedKeys, keys) = collectKeys(rawKeys)
    if (keys.isEmpty) {
      cursorSubject.onCompleted()
      Observable.empty
    } else {
      cmd(_.mget(prefixedKeys: _*))
        .retryWhen(backoffOrCancel)
        .zipWith(keys)((rawValue, key) => new KeyValue(key, rawValue))
        .filter(_ => !it.closed)
        .doOnCompleted(cursorSubject.onNext(Some(cursor)))
    }
  }

  private def collectKeys(rawKeys: Seq[Array[Byte]]): (mutable.Buffer[Array[Byte]], mutable.Buffer[KeyBytes]) = {
    import context.keyCodec._
    val prefixedKeys: mutable.Buffer[Array[Byte]] = new mutable.ArrayBuffer(rawKeys.length)
    val keys: mutable.Buffer[KeyBytes]            = new mutable.ArrayBuffer(rawKeys.length)
    for {
      rawKey <- rawKeys
      parsedKey = wrapKeyBytes(rawKey)
      if predicate(parsedKey)
    } {
      prefixedKeys += prefixKey(rawKey, partition)
      keys += parsedKey
    }
    (prefixedKeys, keys)
  }
}
