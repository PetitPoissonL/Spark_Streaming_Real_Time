package com.bigdata.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis utility class for obtaining Jedis connections and performing Redis operations
 */
object MyRedisUtils {
  var jedisPool : JedisPool = null
  def getJedisFromPoll(): Jedis = {
    if(jedisPool == null){
      // Create a connection pool object
      val host: String = MyPropsUtils(MyConfig.REDIS_HOST)
      val port: String = MyPropsUtils(MyConfig.REDIS_PORT)

      // Connection pool configuration
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //Maximum Connections
      jedisPoolConfig.setMaxIdle(20) //Maximum Idle
      jedisPoolConfig.setMinIdle(20) //Minimum Idle
      jedisPoolConfig.setBlockWhenExhausted(true) //Wait When Busy
      jedisPoolConfig.setMaxWaitMillis(5000) //Wait Duration When Busy ms
      jedisPoolConfig.setTestOnBorrow(true) //Test Connection on Every Acquisition

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }

    jedisPool.getResource
  }
}
