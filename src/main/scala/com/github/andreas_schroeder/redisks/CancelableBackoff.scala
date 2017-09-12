package com.github.andreas_schroeder.redisks

import rx.lang.scala.{Observable, Scheduler}

import scala.concurrent.duration._

class CancelableBackoff(
                         startBackoff: FiniteDuration,
                         maxBackoff: FiniteDuration,
                         val maxTries: Int = 3,
                         scheduler: Scheduler,
                         retryLogger: (Throwable, Int) => Unit = (_,_) => (),
                         failureLogger: (Throwable) => Unit = _ => ()) {

  def backoff(attempts: Observable[Throwable]): Observable[Any] = backoffOrCancelWhen(cancel = false, attempts)

  def backoffOrCancelWhen(cancel: => Boolean, attempts: Observable[Throwable]): Observable[Any] = {
    attempts
      .zipWith(Observable.from(1 to maxTries + 1))(logRetry)
      .filter(i => !cancel && i <= maxTries)
      .flatMap(i => Observable.timer(backoffTime(i), scheduler))
  }

  def backoffTime(tryCount: Int): FiniteDuration = maxBackoff.min((tryCount * tryCount) * startBackoff)

  private def logRetry(ex: Throwable, number: Int): Int = {
    if (number <= maxTries) {
      retryLogger(ex, number)
    } else {
      failureLogger(ex)
    }
    number
  }

}
