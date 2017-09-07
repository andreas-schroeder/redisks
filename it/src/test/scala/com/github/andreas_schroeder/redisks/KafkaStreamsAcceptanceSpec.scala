package com.github.andreas_schroeder.redisks

import java.net.ServerSocket
import java.nio.file.Files
import java.util.{Comparator, Properties}

import com.lambdaworks.redis.{RedisClient, RedisURI}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.StateStoreSupplier
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreTypes}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FeatureSpec, GivenWhenThen, MustMatchers}
import redis.embedded.RedisServer

import scala.collection.JavaConverters._


class KafkaStreamsAcceptanceSpec extends FeatureSpec with MustMatchers with GivenWhenThen with EmbeddedKafka with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(200, Millis)))

  feature("KeyValueStore") {
    scenario("KTable x KTable join") {
      withRunningKafka {
        createTopics("topic-one", "topic-two", "topic-out")
        withRedis { implicit client =>
          withStreamsApp { builder =>
            val topicOne = builder.table[String, String]("topic-one", redis("s1"))
            val topicTwo = builder.table[String, String]("topic-two", redis("s2"))
            topicOne.join(topicTwo, (one: String, two: String) => one + "x" + two).to("topic-out")
          } { streams =>
            Given("a streams app joining two topics")
            When("sending messages to the join input topics")
            send("topic-one", "c" -> "c", "b" -> "b", "a" -> "a")
            send("topic-two", "d" -> "D", "a" -> "A", "b" -> "B")

            Then("joined records are sent to the output topic")
            val messages = consumeNumberStringMessagesFrom("topic-out", 2)
            messages must have size 2
            messages must contain allOf ("axA", "bxB")
          }
        }
      }
    }

    scenario("KTable store") {
      withRunningKafka {
        createTopics("topic-one")
        withRedis { implicit client =>
          withStreamsApp { builder =>
            val topicOne = builder.table[String, String]("topic-one", redis("s1"))
          } { streams =>

            eventually { streams.allMetadata().isEmpty mustBe false }

            Given("a streams app with a KTable and a queryable state store")
            val store = streams.store("s1", QueryableStoreTypes.keyValueStore[String, String]())
            When("sending messages to the KTable input topic")
            send("topic-one", "c" -> "c", "b" -> "b", "a" -> "a")

            Then("the records must be available in the store")
            val expected = Seq("a", "b", "c").map(i => new KeyValue(i, i))

            eventually { store.approximateNumEntries() mustBe >=(3L) }
            val it = store.all()
            it.asScala.toList must contain allElementsOf expected
            it.close()
          }
        }
      }
    }
  }

  private def streamsConfig = {
    val port = embeddedKafkaConfig.kafkaPort
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-app")
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "join-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:$port")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1000")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8000")
    props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams").toString)
    props
  }

  private def withStreamsApp(app: KStreamBuilder => Unit)(body: KafkaStreams => Any): Any = {
    val builder = new KStreamBuilder
    app(builder)
    val streams = new KafkaStreams(builder, streamsConfig)
    streams.start()
    try {
      body(streams)
    } finally {
      streams.close()
    }
  }

  private def redis(name: String)(implicit client: RedisClient): StateStoreSupplier[KeyValueStore[_, _]] = {
    val s = Serdes.String
    RedisStore.keyValueStore(name)
      .withClient(client)
      .withKeys(s)
      .withValues(s)
      .withKeyComparator(Comparator.naturalOrder[String])
      .cached()
      .build
  }

  def withRedis(body: RedisClient => Any) = {
    val port = freePort
    val redisServer = new RedisServer(port)
    redisServer.start()
    val client = RedisClient.create(RedisURI.create("localhost", port))
    try {
      body(client)
    } finally {
      redisServer.stop()
    }
  }

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    freePort,
    freePort,
    Map("offsets.topic.replication.factor" -> "1"),
    Map.empty,
    Map.empty)

  implicit val stringSerializer = new StringSerializer

  private def createTopics(topicNames: String*): Unit =
    topicNames.foreach(name => createCustomTopic(name, partitions = 2))

  private def send(topic: String, records: (String, String)*) =
    for((key, value) <- records) {
      publishToKafka(topic, key, value)
    }

  private def freePort = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
