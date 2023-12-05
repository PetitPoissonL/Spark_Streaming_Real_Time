package com.bigdata.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Business data consumption and sharding
 *
 * 1. Preparing Real-time Processing Environment
 * 2. Read offsets from Redis
 * 3. Consume data from Kafka
 * 4. Extract the offset endpoint
 * 5. Process data
 *  5.1. Transform data structure
 *  5.2. Data Sharding
 *    Fact data -> Kafka
 *    dimension data -> Redis
 * 6. Flush Kafka
 * 7. Commit offsets
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    // 1. Preparing Real-time Processing Environment
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2. Read offsets from Redis
    val topicName : String = "ODS_BASE_DB_1018"
    val groupId : String = "ODS_BASE_DB_GROUP_1018"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    // 3. Consume data from Kafka
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4. Extract the offset endpoint
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5. Process data
    // 5.1. Transform data structure
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        jSONObject
      }
    )
    //jsonObjDStream.print(100)

    // 5.2. Data Sharding
    // TODO: How to dynamically configure the list of tables?
    // List of fact tables
    val factTables : Array[String] = Array[String]("order_info", "order_detail")
    // List of dimension tables
    val dimTables : Array[String] = Array[String]("user_info", "base_province")

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            // Open a Redis connection
            val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
            for (jsonObj <- jsonObjIter){
              // Extract database operation types
              val operType: String = jsonObj.getString("type")
              val opValue: String = operType match {
                case "bootstrap-insert" => "I"  // For historical dimension loading
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              // Determine the operation type:
              //  -> Identify the specific operation.
              //  -> Filter out uninteresting data
              if(opValue != null){
                // Extract table name
                val tableName: String = jsonObj.getString("table")

                if(factTables.contains(tableName)){
                  // fact data
                  val data: String = jsonObj.getString("data")
                  val dwdTopicName : String = s"DWD_${tableName.toUpperCase}_$opValue"  //exp: DWD_ORDER_INFO_I
                  MyKafkaUtils.send(dwdTopicName, data)
                }

                if(dimTables.contains(tableName)){
                  // dimension data
                  // How to store dimension data in Redis?
                  //  -> key: DIM:tableName:ID
                  //  -> value: jsonString of the entire data record
                  //  -> Write API: set
                  //  -> Read API: get
                  //  -> Expiry Status: Does not expire

                  // Extract ID
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey : String = s"DIM:${tableName.toUpperCase}:$id"

                  // The frequent opening and closing of Redis connections in this location
                  //val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                  jedis.set(redisKey, dataObj.toJSONString)
                  // jedis.close()
                }
              }
            }
            // Close the Redis connection
            jedis.close()

            // Flush Kafka
            MyKafkaUtils.flush()
          }
        )
        // Commit offsets
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
