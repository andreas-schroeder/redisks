package com.github.andreas_schroeder.redisks

import org.apache.kafka.streams.KeyValue
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{MustMatchers, Outcome, fixture}
import rx.lang.scala.Notification.{OnCompleted, OnNext}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.concurrent.duration._

class RedisKeyValueIteratorSpec extends fixture.FlatSpec with MustMatchers with ScalaFutures {

  val timeout: FiniteDuration = 100.millis

  behavior of "RedisKeyValueIterator.hasNext"

  it should "return true when next item is available" in { fixture =>
    import fixture._

    putItem("key", "value")

    kvIt.hasNext mustBe true
  }

  it should "return false when queue is completed" in { fixture =>
    import fixture._

    putCompleted()

    kvIt.hasNext mustBe false
  }

  it should "wait for next item if queue is currently empty" in { fixture =>
    import fixture._
    val eventualNext = Future { kvIt.hasNext }

    eventualNext.isReadyWithin(timeout) mustBe false

    putItem("key", "value")

    eventualNext.futureValue mustBe true
  }

  it should "wait for completion if queue is currently empty" in { fixture =>
    import fixture._
    val eventualNext = Future { kvIt.hasNext }

    eventualNext.isReadyWithin(timeout) mustBe false

    putCompleted()

    eventualNext.futureValue mustBe false
  }

  behavior of "RedisKeyValueIterator.next"

  it should "return the next item if it is available" in { fixture =>
    import fixture._
    putItem("key", "value")

    kvIt.next mustBe new KeyValue("key", "value")
  }

  it should "throw a NoSuchElementException when next is requested and queue is completed" in { fixture =>
    import fixture._
    putCompleted()

    a[NoSuchElementException] mustBe thrownBy { kvIt.next }
  }

  it should "wait for next item if queue is currently empty" in { fixture =>
    import fixture._
    val eventualNext = Future { kvIt.next }

    eventualNext.isReadyWithin(timeout) mustBe false

    putItem("key", "value")

    eventualNext.futureValue mustBe new KeyValue("key", "value")
  }

  it should "wait for completion if queue is currently empty" in { fixture =>
    import fixture._
    val eventualNext = Future { kvIt.next }

    eventualNext.isReadyWithin(timeout) mustBe false

    putCompleted()

    val caught = the[TestFailedException] thrownBy { eventualNext.futureValue }
    caught.getCause mustBe a[NoSuchElementException]
  }

  behavior of "RedisKeyValueIterator.peekNextKey"

  it should "peek the next key if it is available" in { fixture =>
    import fixture._
    putItem("key", "value")
    kvIt.peekNextKey mustBe "key"
  }

  it should "throw a NoSuchElementException when next is requested and queue is completed" in { fixture =>
    import fixture._
    putCompleted()
    a[NoSuchElementException] mustBe thrownBy { kvIt.peekNextKey }
  }

  it should "wait for next item if queue is currently empty" in { fixture =>
    import fixture._
    val eventualKey = Future { kvIt.peekNextKey }

    eventualKey.isReadyWithin(timeout) mustBe false

    putItem("key", "value")
    eventualKey.futureValue mustBe "key"
  }

  it should "wait for completion if queue is currently empty" in { fixture =>
    import fixture._
    val eventualKey = Future { kvIt.peekNextKey }

    eventualKey.isReadyWithin(timeout) mustBe false

    putCompleted()

    val caught = the[TestFailedException] thrownBy { eventualKey.futureValue }
    caught.getCause mustBe a[NoSuchElementException]
  }

  def withFixture(test: OneArgTest): Outcome = {
    val fixture = new FixtureParam()

    withFixture(test.toNoArgTest(fixture))
  }

  class FixtureParam() {
    val kvIt = new RedisKeyValueIterator[String, String](2, "store-name", _ => ())

    def putItem(key: String, value: String): Unit = kvIt.queue.put(OnNext(new KeyValue(key, value)))

    def putCompleted(): Unit = kvIt.queue.put(OnCompleted)
  }
}
