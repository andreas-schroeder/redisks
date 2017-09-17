package com.github.andreas_schroeder.redisks

import com.lambdaworks.redis.{RedisClient, RedisURI}
import org.apache.kafka.streams.processor.ProcessorContext
import org.scalatest.{GivenWhenThen, MustMatchers, Outcome, fixture}
import redis.embedded.RedisServer

class RedisStoreAdminAcceptanceSpec
    extends fixture.FeatureSpec
    with MustMatchers
    with GivenWhenThen
    with RedisKeyValueStores {

  case class FixtureParam(redisServer: RedisServer,
                          client: RedisClient,
                          context: ProcessorContext,
                          admin: RedisStoreAdmin)

  feature("clearStore") {
    scenario("clear an existing store") { fixture =>
      import fixture._
      Given("an existing store")
      println("creating the store")
      val store = createStore("store", client, context)
      store.put("key", "value")
      println("closing the store")
      store.close()

      When("clearing the store")
      println("clearing the store")
      admin.clearStore("store", 1)

      Then("the store is empty")
      println("re-creating the store")
      val emptyStore = createStore("store", client, context)
      emptyStore.get("key") mustBe null
    }
  }

  def withFixture(test: OneArgTest): Outcome = {
    val port    = freePort
    val server  = new RedisServer(port)
    val context = createContext
    val client  = RedisClient.create(RedisURI.create("localhost", port))
    server.start()
    val fixture = FixtureParam(server, client, context, new RedisStoreAdmin(RedisConnectionProvider.fromClient(client)))

    try {
      withFixture(test.toNoArgTest(fixture))
    } finally {
      client.shutdown()
      server.stop()
    }
  }
}
