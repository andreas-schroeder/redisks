package com.github.andreas_schroeder.redisks

import com.lambdaworks.redis.RedisConnectionException
import org.mockito.Mockito.verify
import org.scalatest.matchers._
import org.scalatest.{Outcome, flatspec}
import org.scalatestplus.mockito.MockitoSugar
import rx.lang.scala.Observable
import rx.lang.scala.observers.TestSubscriber
import rx.lang.scala.schedulers.TestScheduler
import rx.lang.scala.subjects.PublishSubject

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RetriesSpec extends flatspec.FixtureAnyFlatSpec with must.Matchers with MockitoSugar {

  behavior of "backoffOrCancelWhen"

  it should "retry with backoff" in { fixture =>
    import fixture._
    
    val obs = Observable.just(ex, ex)

    retries.backoffOrCancelWhen(cancel = false, obs).subscribe(sub)

    sub.assertNoValues()
    scheduler.advanceTimeBy(1.second)
    sub.assertValueCount(1)
    scheduler.advanceTimeBy(4.second)
    sub.assertValueCount(2)
  }

  it should "give up after max tries" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, ex, ex, ex)

    retries.backoffOrCancelWhen(cancel = false, obs).subscribe(sub)

    scheduler.advanceTimeBy(20.second)
    sub.assertValueCount(3)
  }

  it should "abort backoff on cancellation" in { fixture =>
    import fixture._
    val subject = PublishSubject[Throwable]()

    var cancel = false

    retries.backoffOrCancelWhen(cancel, subject).subscribe(sub)
    subject.onNext(ex)
    scheduler.advanceTimeBy(1.second)
    subject.onNext(ex)
    scheduler.advanceTimeBy(4.second)
    sub.assertValueCount(2)

    cancel = true
    subject.onNext(ex)
    scheduler.advanceTimeBy(15.second)
    sub.assertValueCount(2)
  }

  it should "abort backoff on unrecoverable exception" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, new RuntimeException, ex, ex)
    retries.backoffOrCancelWhen(cancel = false, obs).subscribe(sub)
    scheduler.advanceTimeBy(20.second)

    sub.assertValueCount(1)
  }

  it should "log retries" in { fixture =>
    import fixture._
    val obs = Observable.just(ex)

    retries.backoffOrCancelWhen(cancel = false, obs).subscribe(sub)

    scheduler.advanceTimeBy(2.seconds)

    verify(retryLogger).apply(ex, 1)
  }

  it should "log failures" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, ex, ex, ex)

    retries.backoffOrCancelWhen(cancel = false, obs).subscribe(sub)

    scheduler.advanceTimeBy(20.seconds)

    verify(failureLogger).apply(ex)
  }

  behavior of "backoffTime"

  it should "start with the initial backoff time" in { fixture =>
    import fixture._
    retries.backoffTime(1) mustBe 1.second
  }

  it should "exponentially increase with tries" in { fixture =>
    import fixture._
    retries.backoffTime(2) mustBe 4.seconds
    retries.backoffTime(3) mustBe 9.seconds
    retries.backoffTime(4) mustBe 16.seconds
  }

  it should "max out at maximum backoff" in { fixture =>
    import fixture._
    retries.backoffTime(20) mustBe 20.seconds
    retries.backoffTime(2000) mustBe 20.seconds
  }

  def withFixture(test: OneArgTest): Outcome = {
    val fixture = new FixtureParam()

    try {
      withFixture(test.toNoArgTest(fixture))
    } finally {
      fixture.threadPool.shutdownNow()
    }
  }

  class FixtureParam() {
    val sub: TestSubscriber[Any]              = TestSubscriber()
    val ex: Throwable                         = new RedisConnectionException("test exception")
    val threadPool: ScheduledExecutorService  = Executors.newScheduledThreadPool(2)
    val ec: ExecutionContext                  = ExecutionContext.fromExecutor(threadPool)
    val scheduler: TestScheduler              = TestScheduler()
    val retryLogger: (Throwable, Int) => Unit = mock[(Throwable, Int) => Unit]
    val failureLogger: Throwable => Unit      = mock[Throwable => Unit]
    val retries: Retries                      = new Retries(1.second, 20.seconds, 3, scheduler, threadPool, ec, retryLogger, failureLogger)
  }
}
