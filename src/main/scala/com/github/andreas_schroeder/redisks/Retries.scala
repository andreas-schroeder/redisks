package com.github.andreas_schroeder.redisks

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.lambdaworks.redis.{RedisCommandTimeoutException, RedisConnectionException, RedisException, RedisFuture}
import rx.lang.scala.{Observable, Scheduler}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal
import Retries._

class Retries(startBackoff: FiniteDuration,
              maxBackoff: FiniteDuration,
              maxTries: Int = 3,
              scheduler: Scheduler,
              threadPool: ScheduledExecutorService,
              futuresExecutionContext: ExecutionContext,
              retryLogger: (Throwable, Int) => Unit = (_, _) => (),
              failureLogger: (Throwable) => Unit = _ => ()) {

  def backoff(attempts: Observable[Throwable]): Observable[Any] = backoffOrCancelWhen(cancel = false, attempts)

  def backoffOrCancelWhen(cancel: => Boolean, attempts: Observable[Throwable]): Observable[Any] = {
    attempts
      .zipWith(Observable.from(1 to maxTries + 1))(logRetry(cancel))
      .takeWhile(b => isRecoverable(b._1))
      .filter(b => !cancel && b._2 <= maxTries)
      .flatMap { case (ex, i) => Observable.timer(backoffTime(i), scheduler) }
  }

  def backoffTime(tryCount: Int): FiniteDuration = maxBackoff.min((tryCount * tryCount) * startBackoff)

  private def logRetry(canceled: Boolean)(ex: Throwable, number: Int): (Throwable, Int) = {
    if (!canceled) {
      if (number <= maxTries && isRecoverable(ex)) {
        retryLogger(ex, number)
      } else {
        failureLogger(ex)
      }
    }
    (ex, number)
  }

  def retrySync[V](f: => V): V = {
    def doTry(tryCount: Int): V =
      try {
        f
      } catch {
        case RecoverableRedisException(ex) if tryCount <= maxTries =>
          retryLogger(ex, tryCount)
          Thread.sleep(backoffTime(tryCount).toMillis)
          doTry(tryCount + 1)
        case NonFatal(t) =>
          failureLogger(t)
          throw t
      }

    doTry(1)
  }

  def retryAsync[T](f: => RedisFuture[T]): Future[T] = {
    implicit val ec: ExecutionContext = futuresExecutionContext
    def doTry(tryCount: Int): Future[T] =
      Future { f.get() }
        .recoverWith {
          case RecoverableRedisException(t) if tryCount <= maxTries =>
            retryLogger(t, tryCount)
            delay(backoffTime(tryCount)).flatMap(_ => doTry(tryCount + 1))
          case t =>
            failureLogger(t)
            throw t
        }

    doTry(1)
  }

  def delay(delay: FiniteDuration): Future[Unit] = {
    val promise = Promise[Unit]()
    threadPool.schedule(() => promise.complete(Try(())), delay.toMillis, TimeUnit.MILLISECONDS)
    promise.future
  }
}

object Retries {

  def isRecoverable(ex: Throwable): Boolean = ex match {
    case _: RedisConnectionException | _: RedisCommandTimeoutException => true
    case _                                                             => false
  }

  object RecoverableRedisException {

    def unapply(ex: Throwable): Option[RedisException] = ex match {
      case e: RedisConnectionException     => Some(e)
      case e: RedisCommandTimeoutException => Some(e)
      case _                               => None
    }
  }
}
