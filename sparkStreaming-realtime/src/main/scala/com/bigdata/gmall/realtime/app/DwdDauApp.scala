package com.bigdata.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.gmall.realtime.bean.{DauInfo, PageLog}
import com.bigdata.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.{lang, util}
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * Daily Active Users (DAU) wide table
 *
 * 1. Preparing Real-time Processing Environment: StreamingContext
 * 2. Read offsets from Redis
 * 3. Consume data from Kafka
 * 4. Extract the offset endpoint
 * 5. Process data
 *    5.1. Transform data structure
 *    5.2. Deduplication
 *    5.3. Dimension association
 * 6. Write to Elasticsearch
 * 7. Commit offsets
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 1.Preparing Real-time Processing Environment: StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2. Read offsets from Redis
    val topicName : String = "DWD_PAGE_LOG_TOPIC_1018"
    val groupId : String = "DWD_DAU_GROUP"
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
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd =>
        println("before self-review: " + rdd.count())
    )

    // 5.2. Deduplication
    // Self-review: Filter out data in page visit data where 'last_page_id' is not empty
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd => {
        println("after self-review: " + rdd.count())
        println("--------------------------------------")
      }
    )

    // Third-party review: Maintain the daily active 'mid' through Redis
    // After self-review, each data record needs to be compared with Redis to deduplicate
    // How to maintain daily active status in Redis?
    //  -> Type: set
    //  -> key: DAU:DATE
    //  -> value: collection of mid
    //  -> Write API: sadd
    //  -> Read API: smembers
    //  -> Expiry Status: 24h
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("before third-party review: " + pageLogList.size)

        // Store the required data
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()

        for (pageLog <- pageLogList) {
          // Extract the 'mid' from each data record (The DAU count is based on 'mid')
          val mid: String = pageLog.mid

          // Retrieve the date (since testing data from different days, you cannot directly obtain the system time)
          val ts: Long = pageLog.ts
          val date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          // Determine if Redis contains the 'mid'
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid) //Atomic operations for checking inclusion and writing
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("after third-party review: " + pageLogs.size)
        pageLogs.iterator
      }
    )
    //redisFilterDStream.print()

    // 5.3. Dimension association
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()

        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          // Copy the existing fields from 'pageLog' to 'DauInfo' (Copy through object copying)
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          // Supplementing dimensions
          // User information dimension
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          val gender: String = userInfoJsonObj.getString("gender")
          val birthday: String = userInfoJsonObj.getString("birthday")
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          // Location information dimension
          val provinceId: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceId"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")

          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          // Date field processing
          val date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)

          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
    //dauInfoDStream.print(100)

    // Writing into OLAP
    // Splitting indices by date and controlling mappings, settings, aliases, etc., through an index template
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList

            if(docs.nonEmpty){
              // index name
              // If it's a production environment, you can directly obtain the current date
              // Because we are simulating data, we will generate data for different days
              // Get the date from the first data record
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
              val indexName: String = s"gmall_dau_info_1018_$dateStr"
              // write in ES
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        // commit offset
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
