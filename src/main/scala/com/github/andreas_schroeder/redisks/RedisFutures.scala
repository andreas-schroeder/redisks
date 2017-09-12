package com.github.andreas_schroeder.redisks

import java.util.concurrent.{Executors, TimeUnit}

import com.lambdaworks.redis.RedisFuture

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait RedisFutures {

  private val scheduler = Executors.newScheduledThreadPool(20)

  private val futuresExecutionContext = ExecutionContext.fromExecutor(scheduler)

  def asScalaWithRetry[T](f: => RedisFuture[T], backoff: Int => FiniteDuration, maxTries: Int): Future[T] = {
    implicit val ec: ExecutionContext = futuresExecutionContext
    def doTry(tryCount: Int): Future[T] =
      Future { f.get() }
        .recoverWith { case RecoverableRedisException(_) if tryCount <= maxTries  =>
          delay(backoff(tryCount)).flatMap(_ => doTry(tryCount + 1))
        }

    doTry(1)
  }

  def delay(delay: FiniteDuration): Future[Unit] = {
    val promise = Promise[Unit]()
    scheduler.schedule(() => promise.complete(Try(())), delay.toMillis, TimeUnit.MILLISECONDS)
    promise.future
  }

  def shutdown(): Unit = scheduler.shutdownNow()

}
