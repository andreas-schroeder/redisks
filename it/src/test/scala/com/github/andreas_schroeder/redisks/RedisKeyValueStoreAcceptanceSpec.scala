package com.github.andreas_schroeder.redisks

import java.lang.{NullPointerException => NPE}
import java.util.UUID
import java.util.concurrent.TimeoutException

import com.lambdaworks.redis.{RedisClient, RedisURI}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.mockito.Mockito.when

import scala.concurrent.duration._
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest._
import redis.embedded.RedisServer

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

class RedisKeyValueStoreAcceptanceSpec
    extends fixture.FeatureSpec
    with BeforeAndAfterAll
    with RedisKeyValueStores
    with GivenWhenThen
    with MockitoSugar
    with MustMatchers
    with Eventually {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))

  case class FixtureParam(context: ProcessorContext,
                          store: RedisKeyValueStore[String, String],
                          secondStore: RedisKeyValueStore[String, String])

  feature("put and get") {
    scenario("Value is not set") { f =>
      Given("an empty key-value store")

      When("any entry is retrieved")
      val value = f.store.get("key")

      Then("the value must be null")
      value mustBe null
    }

    scenario("One key stored") { fixture =>
      import fixture._

      Given("A store with an entry for key 'k'")
      store.put("k", "value")
      eventually { store.approximateNumEntries mustBe 1 }

      When("the value for 'k' is retrieved")
      val v = store.get("k")

      Then("the value must be 'value'")
      v mustBe "value"
    }

    scenario("Two keys stored") { fixture =>
      import fixture._

      When("adding two entries with different values")
      store.put("key1", "value1")
      store.put("key2", "value2")
      eventually { store.approximateNumEntries mustBe 2 }

      Then("retrieving the entries returns the stored values")
      store.get("key1") mustBe "value1"
      store.get("key2") mustBe "value2"
    }
  }

  feature("Sharing redis") {
    scenario("Store for two partitions") { implicit fixture =>
      import fixture._
      When("adding entries for different partitions")
      setContextPartition(0)
      store.put("key1", "value1")
      eventually { store.approximateNumEntries mustBe 1 }

      setContextPartition(1)
      store.put("key2", "value2")
      eventually { store.approximateNumEntries mustBe 1 }

      Then("the entries are only visible for their respective partition store")
      setContextPartition(0)
      store.get("key1") mustBe "value1"
      store.get("key2") mustBe null

      setContextPartition(1)
      store.get("key1") mustBe null
      store.get("key2") mustBe "value2"
    }

    scenario("Two different stores") { implicit fixture =>
      import fixture._

      When("adding entries to different stores")
      store.put("key1", "value1")
      secondStore.put("key2", "value2")
      eventually { store.approximateNumEntries mustBe 1 }
      eventually { secondStore.approximateNumEntries mustBe 1 }

      Then("the entries are only visible in the respective store")
      store.get("key1") mustBe "value1"
      store.get("key2") mustBe null

      secondStore.get("key1") mustBe null
      secondStore.get("key2") mustBe "value2"
    }
  }

  feature("putIfAbsent") {
    scenario("A value is already associated") { fixture =>
      import fixture._

      Given("A store with an entry for key 'k'")
      store.put("k", "value 1")
      eventually { store.approximateNumEntries mustBe 1 }

      When("calling putIfAbsent with that key")
      val current = store.putIfAbsent("k", "value 2")

      Then("the old value is returned")
      current mustBe "value 1"

      And("the associated value is not updated")
      store.get("k") mustBe "value 1"
    }

    scenario("No value is associated yet") { fixture =>
      import fixture._
      Given("A store with no for key 'k'")
      When("calling putIfAbsent with that key")
      val current = store.putIfAbsent("k", "value 2")

      Then("null is returned")
      current mustBe null

      And("the associated value is updated")
      store.get("k") mustBe "value 2"
    }

    scenario("null key is passed") { fixture =>
      import fixture._

      When("calling putIfAbsent with a null key")
      Then("a NPE is thrown")
      a[NPE] must be thrownBy store.putIfAbsent(null, "value")
    }

    scenario("null value is passed") { fixture =>
      import fixture._

      When("calling putIfAbsent with a null value")
      Then("a NPE is thrown")
      a[NPE] must be thrownBy store.putIfAbsent("key", null)
    }
  }

  feature("delete") {
    scenario("delete existing entry") { fixture =>
      import fixture._

      Given("A store with an entry for key 'k'")
      store.put("k", "value")
      eventually { store.approximateNumEntries mustBe 1 }

      When("deleting this entry")
      val value = store.delete("k")

      Then("the value is returned")
      value mustBe "value"

      And("the value is removed from the store")
      store.get("key") mustBe null
    }

    scenario("delete non-existing entry") { fixture =>
      import fixture._

      When("deleting a non-existing entry")
      val value = store.delete("k")

      Then("null is returned")
      value mustBe null
    }

    scenario("Delete key from one of multiple partitions") { implicit fixture =>
      import fixture._

      Given("a store with an entry for key 'k' in two partitions")
      setContextPartition(0)
      store.put("k", "value1")
      eventually { store.approximateNumEntries mustBe 1 }

      setContextPartition(1)
      store.put("k", "value2")
      eventually { store.approximateNumEntries mustBe 1 }

      When("deleting the entry for one partition")
      setContextPartition(0)
      store.delete("k")

      Then("the second partition is unaffected")
      setContextPartition(1)
      store.get("k") mustBe "value2"
    }
  }

  feature("putAll") {
    scenario("Stores all entries") { fixture =>
      import fixture._

      When("adding multiple entries")
      store.putAll(List("a", "b", "c").map(i => new KeyValue(i, i)).asJava)

      eventually { store.approximateNumEntries mustBe 3 }

      Then("all added entries can be retrieved")
      store.get("a") mustBe "a"
      store.get("b") mustBe "b"
      store.get("c") mustBe "c"
    }
  }

  feature("all") {
    scenario("iterates all entries") { fixture =>
      import fixture._

      When("adding multiple entries")
      val kvs = createKeyValues(200)
      store.putAll(kvs.asJava)

      eventually { store.approximateNumEntries mustBe 200 }

      Then("all added entries can be retrieved")
      val resultKvs = store.all().asScala.to[Seq]
      resultKvs must have size kvs.size
      resultKvs must contain allElementsOf kvs
    }

    scenario("iterates no entries") { fixture =>
      import fixture._
      Given("an empty store")
      Then("iterating all entries yields no entries, but completes")
      val all = store.all()
      all.hasNext mustBe false
      all.hasNext mustBe false
      all.close()
    }

    scenario("aborting iteration") { fixture =>
      import fixture._

      Given("a store with entries")
      val kvs = createKeyValues(200)
      store.putAll(kvs.asJava)

      eventually { store.approximateNumEntries() mustBe 200 }

      When("aborting the iteration")
      val it = store.all()
      it.next()
      it.next()
      it.next()
      it.close()

      Then("the store remains operational")
      store.get("k3") mustBe "v3"
    }
  }

  feature("approximateNumEntries") {
    scenario("Store with entries") { fixture =>
      import fixture._

      Given("a store with entries")
      store.putAll(createKeyValues(200).asJava)

      Then("approximateNumEntries returns the number of entries")
      eventually { store.approximateNumEntries() mustBe 200 }
    }
  }

  feature("range") {
    scenario("produces elements in range") { fixture =>
      import fixture._

      Given("a store with entries")
      Seq("a", "b", "c", "e").foreach(i => store.put(i, i))

      Then("range returns the entries in the requested range")
      val result   = store.range("b", "d").asScala.to[Seq]
      val expected = Seq("b", "c").map(i => new KeyValue(i, i))
      result must have size 2
      result must contain allElementsOf expected
    }
  }

  feature("close") {
    scenario("closes all open iterators") { fixture =>
      import fixture._

      Given("a store with open iterators")
      val it = store.all().asInstanceOf[RedisKeyValueIterator[String, String]]
      it.closed mustBe false

      When("closing the store")
      store.close()

      Then("the open iterators are closed")
      it.closed mustBe true
    }

    scenario("waits for pending operations") { fixture =>
      import fixture._
      import scala.concurrent.ExecutionContext.Implicits._
      val p = Promise[Any]()

      Given("a store with pending operations")
      store.addPendingOperations(p.future)

      When("closing the store")
      val eventuallyClosed = Future { store.close() }

      Then("the store waits for completion of the operations")
      a[TimeoutException] mustBe thrownBy {
        Await.ready(eventuallyClosed, 500.millis)
      }
      p.complete(Success(()))
      Await.ready(eventuallyClosed, 500.millis)
    }
  }

  private def createKeyValues(count: Int) =
    (1 to count).map(i => new KeyValue(s"k$i", s"v$i"))

  val redisPort = freePort

  val server = new RedisServer(redisPort)

  @volatile var client: RedisClient = _

  override protected def beforeAll(): Unit = {
    server.start()
    client = RedisClient.create(RedisURI.create("localhost", redisPort))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
    server.stop()
  }

  def randomName: String = UUID.randomUUID.toString

  def withFixture(test: OneArgTest): Outcome = {
    val context = createContext
    val fixture =
      FixtureParam(context, createStore(randomName, client, context), createStore(randomName, client, context))

    try {
      withFixture(test.toNoArgTest(fixture))
    } finally {
      fixture.store.close()
      fixture.secondStore.close()
    }
  }

  private def setContextPartition(partition: Int)(implicit f: FixtureParam): Unit = {
    when(f.context.partition).thenReturn(partition)
  }
}
