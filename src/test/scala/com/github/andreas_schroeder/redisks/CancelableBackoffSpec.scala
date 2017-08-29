package com.github.andreas_schroeder.redisks

import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito.verify
import org.scalatest.{MustMatchers, Outcome, fixture}
import rx.lang.scala.Observable
import rx.lang.scala.observers.TestSubscriber
import rx.lang.scala.schedulers.TestScheduler
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.duration._

class CancelableBackoffSpec extends fixture.FlatSpec with MustMatchers with MockitoSugar {

  behavior of "backoffOrCancelWhen"

  it should "retry with backoff" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, ex)

    backoff.backoffOrCancelWhen(false, obs).subscribe(sub)

    sub.assertNoValues()
    scheduler.advanceTimeBy(1.second)
    sub.assertValueCount(1)
    scheduler.advanceTimeBy(4.second)
    sub.assertValueCount(2)
  }

  it should "give up after max tries" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, ex, ex, ex)

    backoff.backoffOrCancelWhen(false, obs).subscribe(sub)

    scheduler.advanceTimeBy(20.second)
    sub.assertValueCount(3)
  }

  it should "abort backoff on cancellation" in { fixture =>
    import fixture._
    val subject = PublishSubject[Throwable]()

    var cancel = false

    backoff.backoffOrCancelWhen(cancel, subject).subscribe(sub)
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

  it should "log retries" in { fixture =>
    import fixture._
    val obs = Observable.just(ex)

    backoff.backoffOrCancelWhen(false, obs).subscribe(sub)

    scheduler.advanceTimeBy(2.seconds)

    verify(retryLogger).apply(ex, 1)
  }

  it should "log failures" in { fixture =>
    import fixture._
    val obs = Observable.just(ex, ex, ex, ex)

    backoff.backoffOrCancelWhen(false, obs).subscribe(sub)

    scheduler.advanceTimeBy(20.seconds)

    verify(failureLogger).apply(ex)
  }

  def withFixture(test: OneArgTest): Outcome = {
    val fixture = new FixtureParam()

    withFixture(test.toNoArgTest(fixture))
  }

  class FixtureParam() {
    val sub: TestSubscriber[Any] = TestSubscriber()
    val ex: Throwable = new RuntimeException
    val scheduler = TestScheduler()
    val retryLogger = mock[(Throwable, Int) => Unit]
    val failureLogger = mock[Throwable => Unit]
    val backoff: CancelableBackoff =
      new CancelableBackoff(1.second, 10.seconds, 3, scheduler, retryLogger, failureLogger)
  }
}