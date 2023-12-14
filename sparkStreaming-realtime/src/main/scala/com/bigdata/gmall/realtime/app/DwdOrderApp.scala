package com.bigdata.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.bigdata.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.SerializableConfiguration
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 * Order wide table task
 *
 * 1. Preparing Real-time Processing Environment: StreamingContext
 * 2. Read offsets from Redis * 2 (order & order detail)
 * 3. Consume data from Kafka * 2 (order & order detail)
 * 4. Extract the offset * 2 (order & order detail)
 * 5. Process data
 *    5.1. Transform data structure
 *    5.2. Dimension association
 *    5.3. Double stream join
 * 6. Write to Elasticsearch
 * 7. Commit offsets * 2  (order & order detail)
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    // 1. Preparing Real-time Processing Environment
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2. Read offsets from Redis
    // order_info
    val orderInfoTopicName : String = "DWD_ORDER_INFO_I_1018"
    val orderInfoGroup : String = "DWD_ORDER_INFO_GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroup)

    // order_detail
    val orderDetailTopicName : String = "DWD_ORDER_DETAIL_I_1018"
    val orderDetailGroup : String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    // 3. Consume data from Kafka
    // order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsets != null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }

    // order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsets != null && orderDetailOffsets.nonEmpty){
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    }else{
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    // 4. Extract the offset
    // order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5. Process data
    // 5.1. Transform data structure
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //orderInfoDStream.print(100)

    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
    //orderDetailDStream.print(100)

    // 5.2. Dimension association
    // order_info
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
        for (orderInfo <- orderInfos) {
          // Associate with user dimension
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)

          val gender: String = userInfoJsonObj.getString("gender")
          val birthday: String = userInfoJsonObj.getString("birthday")
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          orderInfo.user_gender = gender
          orderInfo.user_age = age

          // Associate with location dimension
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode

          // Processing date field
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)

          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    orderInfoDimDStream.print(100)

    // 5.3. Double stream join
    // Stream join can only be performed on data from the same batch
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.id, orderDetail))
    // Using full outer join to ensure that all data can be successfully joined
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {

          if (orderInfoOp.isDefined) {
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              val orderDetail: OrderDetail = orderDetailOp.get
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            }

            // Write orderInfo into Redis
            // type: string
            // key: ORDERJOIN:ORDER_INFO:ID
            // value: json
            // write API: set
            // read API: get
            // Expiry Status: 24h
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            //jedis.set(redisOrderInfoKey, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            //jedis.expire(redisOrderInfoKey, 24*3600)
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            // Read orderDetail cache
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                val orderWide = new OrderWide(orderInfo, orderDetail)
                orderWides.append(orderWide)
              }
            }

          } else {
            val orderDetail: OrderDetail = orderDetailOp.get
            // Read orderInfo cache
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.size > 0) {
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            }

            // Write orderDetail into Redis
            // type: set
            // key: ORDERJOIN:ORDER_DETAIL:ORDER_ID
            // value: json, json, ...
            // write API: sadd
            // read API: smembers
            // Expiry Status: 24h
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
            jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
            jedis.expire(redisOrderDetailKey, 24 * 3600)
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    orderWideDStream.print(100)


    ssc.start()
    ssc.awaitTermination()
  }
}
