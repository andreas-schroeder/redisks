package com.github.andreas_schroeder.redisks

import com.lambdaworks.redis.{RedisClient, RedisURI}
import org.scalatest.FeatureSpec
import org.scalatest.tags.Slow
import redis.embedded.RedisServer

import scala.util.Random

@Slow
class RedisKeyValueStoreBenchmark extends FeatureSpec with RedisKeyValueStores {

  feature("Performance") {
    scenario("Benchmark") {
      val port = freePort
      val server = RedisServer.builder().setting("""save """"").port(port).build()
      val context = createContext
      val client = RedisClient.create(RedisURI.create("localhost", port))
      server.start()

      def runBenchmark(entriesCount: Int, entryBytes: Int) = {
        val keyValues = createKeyValues(entriesCount, entryBytes)
        val all = for (_ <- 0 to 3) yield {
          System.gc()
          val store = createStore("store", client, context)
          val putStart = System.currentTimeMillis()
          keyValues.foreach(kv => store.put(kv._1, kv._2))
          store.flush()
          while (store.approximateNumEntries < entriesCount) {
            Thread.sleep(50)
          }
          val putDuration = System.currentTimeMillis() - putStart
          System.gc()
          val getStart = System.currentTimeMillis()
          keyValues.foreach(kv => store.get(kv._1))
          val getDuration = System.currentTimeMillis() - getStart
          val r = BenchmarkResults(getDuration, putDuration, entriesCount, entryBytes)
          println(r.reportLine)
          r
        }

        all.drop(1)
      }

      val r100  = BenchmarkResults.average(runBenchmark(20000, 100))
      val r1024 = BenchmarkResults.average(runBenchmark(20000, 1024))
      val r2048 = BenchmarkResults.average(runBenchmark(20000, 2048))
      val r4000 = BenchmarkResults.average(runBenchmark(20000, 4000))

      println(r100.reportLine)
      println(r1024.reportLine)
      println(r2048.reportLine)
      println(r4000.reportLine)
    }
  }

  def createKeyValues(size: Int, entryBytes: Int): Map[String, String] = {
    val rnd = new Random()

    var keys = Set[String]()
    while(keys.size < size) {
      keys += rnd.nextString(24)
    }
    keys.map(k => (k, rnd.nextString(entryBytes - 24))).toMap
  }
}

object BenchmarkResults {
  def average(results: Seq[BenchmarkResults]): BenchmarkResults = {
    def avg(f: BenchmarkResults => Double): Double =
      results.map(r => f(r)).sum / results.length

    val r = results.head

    BenchmarkResults(avg(_.getDurationMs).toLong, avg(_.putDurationMs).toLong, r.entriesCount, r.entryBytes)
  }
}

case class BenchmarkResults(getDurationMs: Long, putDurationMs: Long, entriesCount: Int, entryBytes: Int) {
  val totalBytes = entriesCount * entryBytes

  val putThroughput = toMbPerSec(putDurationMs)
  val getThroughput = toMbPerSec(getDurationMs)

  val reportLine: String =
    f"entry size: $entryBytes%4d\tput: $putThroughput%4.2f MiB/Sec $putDurationMs%5d ms\tget: $getThroughput%4.2f MiB/Sec $getDurationMs%5d ms"

  private def toMbPerSec(durationMs: Long): Double = 1000.0 * totalBytes / (durationMs * 1024 * 1024)
}

