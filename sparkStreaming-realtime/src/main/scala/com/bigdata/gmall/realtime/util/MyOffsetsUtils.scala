package com.bigdata.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 * Offset management utility class for reading and storing offsets in Redis.
 *
 * Management solution:
 *  1. Post-offset -> Manually control offset submission
 *  2. Manual offset submission -> Spark Streaming provides a manual submission approach, but we cannot use it because we will be transforming the structure of DStreams.
 *  3. Manually extract offsets and maintain them in Redis
 *    -> When consuming data from Kafka, extract the offsets first
 *    -> Once the data is successfully written, store the offsets in Redis
 *    -> Before consuming data from Kafka, retrieve the offsets from Redis and use the retrieved offsets to consume data from Kafka
 */
object MyOffsetsUtils {
  /**
   * store offsets in Redis
   *
   * Where do the stored offsets come from?
   *  -> They are extracted from the consumed data and passed to this method
   *  -> offsetRanges: Array[OffsetRange]
   * What is the structure of the offset?
   *  -> The offset structure is maintained by Kafka:
   *      groupId + topic + partition => offset
   * How are they stored in Redis?
   *  type: hash
   *  key: groupId + topic
   *  value: partition-offset, partition-offset, ...
   *  Write API: hset / hmset
   *  Read API: hgetall
   *  Expiry Status: Does not expire
   */
  def saveOffset(topic : String, groupId : String, offsetRanges: Array[OffsetRange]): Unit = {
    if(offsetRanges != null && offsetRanges.length > 0){
      val offsets = new util.HashMap[String, String]()

      for (offsetRange <- offsetRanges){
        val partition: Int = offsetRange.partition
        val endOffset: Long = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      println("Commit the offset: " + offsets)
      // Store in Redis
      val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
      val redisKey : String = s"offsets:$topic:$groupId"
      jedis.hset(redisKey, offsets)

      jedis.close()
    }
  }

  /**
   * retrieve offsets from Redis
   *
   * How to make Spark Streaming consume data with a specified offset?
   *
   * What is the format of offsets required by Spark Streaming?
   *  -> Map[TopicPartition, Long]
   */
  def readOffset(topic : String, groupId : String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
    val redisKey: String = s"offsets:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("Read the offset: " + offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // Convert a Java map to a Scala map for iteration
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val tp = new TopicPartition(topic, partition.toInt)
      results.put(tp, offset.toLong)
    }

    jedis.close()
    results.toMap
  }
}
