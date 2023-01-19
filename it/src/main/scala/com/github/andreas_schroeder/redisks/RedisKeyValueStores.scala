package com.github.andreas_schroeder.redisks

import com.github.andreas_schroeder.redisks.RedisKeyValueStores._
import com.lambdaworks.redis.RedisClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl
import org.apache.kafka.streams.processor.{ProcessorContext, TaskId}
import org.apache.kafka.streams.state.KeyValueStore
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import java.net.ServerSocket
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit


object RedisKeyValueStores {
  private def streamsConfig: StreamsConfig = {
    val defaults = new ConfigDef().defaultValues()
    defaults.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application")
    defaults.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1")
    new StreamsConfig(defaults)
  } 
  private val JMX_PREFIX = "kafka.streams"
}

trait RedisKeyValueStores extends MockitoSugar {

  def freePort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def clientId = "clientId"

  private def getMetrics(config: StreamsConfig, time: Time, clientId: String): Metrics = {
    val metricConfig: MetricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG)).recordLevel(Sensor.RecordingLevel.forName(config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG))).timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
    val reporters: util.List[MetricsReporter] = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[MetricsReporter], Collections.singletonMap(StreamsConfig.CLIENT_ID_CONFIG, clientId).asInstanceOf[util.Map[String,Object]])
    val jmxReporter: JmxReporter = new JmxReporter
    jmxReporter.configure(config.originals)
    reporters.add(jmxReporter)
    val metricsContext: MetricsContext = new KafkaMetricsContext(JMX_PREFIX, config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    new Metrics(metricConfig, reporters, time, metricsContext)
  }

  def createContext: ProcessorContext = {
    val context = mock[ProcessorContext]
    val metrics = getMetrics(streamsConfig, Time.SYSTEM, clientId)
    val streamsMetricsInstance = new StreamsMetricsImpl(metrics, clientId,
      streamsConfig.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG), Time.SYSTEM)
    val streamsMetrics = spy(streamsMetricsInstance)
    
    when(context.applicationId).thenReturn("application")
    when(context.topic).thenReturn("topic")
    when(context.partition).thenReturn(0)
    when(context.metrics()).thenReturn(streamsMetrics)
    when(context.taskId()).thenReturn(new TaskId(0, 0))
    context
  }

  def createStore(prefix: String, client: RedisClient, context: ProcessorContext): KeyValueStore[String, String] = {
    val store = RedisStore.builder(prefix,
      RedisConfigSetter.fromClient(client),
      Serdes.String(),
      Serdes.String())
      .withLoggingDisabled()
      .build()
    store.init(context, null)
    store
  }
}
