package com.github.andreas_schroeder.redisks

import com.lambdaworks.redis.{RedisClient, RedisURI}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams.State._
import org.apache.kafka.streams.KafkaStreams.{State, StateListener}
import org.apache.kafka.streams._
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.kstream.{KTable, Materialized}
import org.apache.kafka.streams.state.{QueryableStoreType, QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import redis.embedded.RedisServer

import java.net.ServerSocket
import java.nio.file.Files
import java.util.Properties
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class KafkaStreamsAcceptanceSpec
  extends AnyFeatureSpec
    with Matchers
    with GivenWhenThen
    with EmbeddedKafka
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(200, Millis)))

  Feature("KeyValueStore") {
    Scenario("KTable x KTable join") {
      withRunningKafka {
        createTopics("topic-one", "topic-two", "topic-out")
        eventually {
          listTopics() mustBe Set("topic-one", "topic-two", "topic-out")
        }

        withRedis { implicit client =>
          withStreamsApp { builder =>
            val topicOne: KTable[String, String] =
              builder.table("topic-one", Materialized.as(redis("s1"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
                .withLoggingDisabled())
            val topicTwo: KTable[String, String] =
              builder.table("topic-two", Materialized.as(redis("s2"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled()
                .withLoggingDisabled())
            topicOne
              .join[String, String](topicTwo, (value1: String, value2: String) => value1 + "x" + value2)
              .toStream().to("topic-out")
          } { _ =>
            Given("a streams app joining two topics")
            When("sending messages to the join input topics")
            send("topic-one", "c" -> "c", "b" -> "b", "a" -> "a")
            send("topic-two", "d" -> "D", "a" -> "A", "b" -> "B")

            Then("joined records are sent to the output topic")
            val messages = consumeNumberMessagesFromTopics(Set("topic-out"), 2, timeout = 20.seconds).apply("topic-out")
            messages must have size 2
            messages must contain allOf("axA", "bxB")
          }
        }
      }
    }

    Scenario("KTable store") {
      withRunningKafka {
        createTopics("topic-one")
        withRedis { implicit client =>
          withStreamsApp(_.table("topic-one", Materialized.as(redis("s1"))
            .withValueSerde(Serdes.String())
            .withKeySerde(Serdes.String())
            .withCachingEnabled()
            .withLoggingDisabled())) { streams =>
            eventually {
              streams.streamsMetadataForStore("s1").isEmpty mustBe false
            }

            Given("an app with a KTable and a queryable state store")
            val store = streams.store(StoreQueryParameters.fromNameAndType("s1", QueryableStoreTypes.keyValueStore()))
            When("sending messages to the KTable input topic")
            send("topic-one", "c" -> "c", "b" -> "b", "a" -> "a")

            Then("the records must be available in the store")
            val expected = Seq("a", "b", "c").map(i => new KeyValue(i, i))

            eventually {
              store.approximateNumEntries() mustBe >=(3L)
            }
            val it = store.all()
            it.asScala.toList must contain allElementsOf expected
            it.close()
          }
        }
      }
    }

    Scenario("Application restart") {
      withRunningKafka {
        createTopics("topic-one", "topic-two")
        withRedis { implicit client =>
          Given("an app with a state store")
          withStreamsApp(_.table("topic-one", Materialized.as(redis("s1"))
            .withValueSerde(Serdes.String())
            .withKeySerde(Serdes.String())
            .withCachingEnabled()
            .withLoggingDisabled())) { streams =>
            eventually {
              streams.streamsMetadataForStore("s1").isEmpty mustBe false
            }
            send("topic-one", "c" -> "c", "b" -> "b", "a" -> "a")
            val store = streams.store(StoreQueryParameters.fromNameAndType("s1", QueryableStoreTypes.keyValueStore()))
            eventually {
              store.approximateNumEntries() mustBe >=(3L)
            }
          }

          When("restarting the app")
          // using an empty topic ensures that the state store content originates from the previous app
          withStreamsApp(_.table("topic-two", Materialized.as(redis("s1"))
            .withValueSerde(Serdes.String())
            .withKeySerde(Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled())) { streams =>
            eventually {
              streams.streamsMetadataForStore("s1").isEmpty mustBe false
            }

            Then("the state survives the app restart")
            val store = streams.store(StoreQueryParameters.fromNameAndType("s1", QueryableStoreTypes.keyValueStore()))
            eventually {
              store.approximateNumEntries() mustBe >=(3L)
            }
          }
        }
      }
    }
  }

  val keyValueStore: QueryableStoreType[ReadOnlyKeyValueStore[String, String]] = QueryableStoreTypes.keyValueStore[String, String]()

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

  private def waitOnRunning(body: KafkaStreams => Any, streams: KafkaStreams): StateListener =
    (newState: State, oldState: State) => newState match {
      case RUNNING => try {
        body(streams)
      } finally {
        streams.close()
      }
      case _ => // do nothing
    }

  private def withStreamsApp(app: StreamsBuilder => Unit)(body: KafkaStreams => Any): Any = {
    val builder = new StreamsBuilder()
    app(builder)
    val streams = new KafkaStreams(builder.build(), streamsConfig)
    streams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler {
      override def handle(exception: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = throw exception
    })
    streams.setStateListener(waitOnRunning(body, streams))
    streams.start()
  }

  private def redis(name: String)(implicit client: RedisClient): RedisKeyValueBytesStoreSupplier = {
    RedisStore.byteStoreSupplier(name, RedisConfigSetter.fromClient(client))
  }

  def withRedis(body: RedisClient => Any): Any = {
    val port = freePort
    val redisServer = new RedisServer(port)
    redisServer.start()
    val client = RedisClient.create(RedisURI.create("localhost", port))
    try {
      body(client)
    } finally {
      client.shutdown()
      redisServer.stop()
    }
  }

  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(freePort, freePort, Map("offsets.topic.replication.factor" -> "1"), Map.empty, Map.empty)

  implicit val stringSerializer: StringSerializer = new StringSerializer

  implicit val stringDeserializer: StringDeserializer = new StringDeserializer

  private def createTopics(topicNames: String*): Unit =
    topicNames.foreach(name => createCustomTopic(name, partitions = 2))

  private def listTopics(): Set[String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + embeddedKafkaConfig.kafkaPort)
    val consumer = new KafkaConsumer[String, String](props, stringDeserializer, stringDeserializer)
    val topics = consumer.listTopics().asScala.keys.toSet
    consumer.close()
    topics
  }

  private def send(topic: String, records: (String, String)*): Unit =
    for ((key, value) <- records) {
      publishToKafka(topic, key, value)
    }

  private def freePort = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
