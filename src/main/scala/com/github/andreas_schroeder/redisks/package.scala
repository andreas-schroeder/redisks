package com.github.andreas_schroeder

import com.lambdaworks.redis.{RedisCommandTimeoutException, RedisConnectionException, RedisException}

package object redisks {

  object RecoverableRedisException {
    def unapply(ex: Throwable): Option[RedisException] = ex match {
      case e: RedisConnectionException => Some(e)
      case e: RedisCommandTimeoutException => Some(e)
      case _ => None
    }
  }
}
