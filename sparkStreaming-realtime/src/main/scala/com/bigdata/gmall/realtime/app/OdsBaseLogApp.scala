package com.bigdata.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bigdata.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.bigdata.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/**
 * Log Data Consumption and Sharding
 * 1. Preparing Real-time Processing Environment: StreamingContext
 *
 * 2. Consume data from Kafka
 *
 * 3. Process data
 *  3.1. Data structure
 *    Dedicated Structure: Bean
 *    Generic Structure: Map, JsonObject, ...
 *  3.2. Data Sharding
 *
 * 4. Writing to the Data Warehouse Detail(DWD) layer
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 1. Preparing Real-time Processing Environment
    // Note the correspondence between parallelism and the number of partitions in Kafka topics
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2. Consume data from Kafka
    val topicName : String = "ODS_BASE_LOG_1018" // Corresponding to the topic name in the generator configuration
    val groupId : String = "ODS_BASE_LOG_GROUP_1018"

    // Consuming with a specified offset from Redis
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      // Consuming with a specified offset
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      // Consuming with the default offset
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }


    // Extracting offsets from the currently consumed data without processing the data in the stream
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // Executed on the driver side
        rdd
      }
    )

    // 3. Process data
    // 3.1. Data structure transformation
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        // Retrieve the 'value' from the ConsumerRecord; the 'value' is the log data
        val log: String = consumerRecord.value()
        // Convert to a JSON object
        val jsonObj: JSONObject = JSON.parseObject(log)
        // return
        jsonObj
      }
    )
    // jsonObjDStream.print(100)

    // 3.2. Data Sharding
    // log data:
    //  page visit data:
    //    common fields
    //    exposure data
    //    event data
    //    error data
    //  startup data:
    //    common fields
    //    startup data
    //    error data

    val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC_1018" // page visits
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC_1018" // page exposures
    val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC_1018" // page events
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC_1018" // startup data
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC_1018" // error data

    // sharding rules:
    //  Error data: No splitting, send the entire record to the corresponding topic if it contains an error field
    //  Page data: Split into page visits, exposures, and events, and send them to their respective topics
    //  Startup data: Send to the corresponding topic
    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              // Sharding process
              // Sharding error data
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if (errObj != null) {
                // Send error data to the topic DWD_ERROR_LOG_TOPIC
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
              } else {
                // Extract common fields
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar")
                val uid: String = commonObj.getString("uid")
                val os: String = commonObj.getString("os")
                val ch: String = commonObj.getString("ch")
                val isNew: String = commonObj.getString("is_new")
                val md: String = commonObj.getString("md")
                val mid: String = commonObj.getString("mid")
                val vc: String = commonObj.getString("vc")
                val ba: String = commonObj.getString("ba")

                // Extract timestamp
                val ts: Long = jsonObj.getLong("ts")

                // page visit data
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  // Extract page data
                  val pageId: String = pageObj.getString("page_id")
                  val pageItem: String = pageObj.getString("item")
                  val pageItemType: String = pageObj.getString("item_type")
                  val duringTime: Long = pageObj.getLong("during_time")
                  val lastPageId: String = pageObj.getString("last_page_id")
                  val sourceType: String = pageObj.getString("source_type")

                  // Encapsulate as PageLog
                  var pageLog: PageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)

                  // Send to the topic DWD_PAGE_LOG_TOPIC
                  // pageLog is a Scala object without get and set methods, so we need SerializeConfig to retrieve data from its fields
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                  // Extract exposure data
                  val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                  if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                    for (i <- 0 until displaysJsonArr.size()) {
                      // To iterate and retrieve each exposure data
                      val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                      // Extract exposure fields
                      val displayType: String = displayObj.getString("display_type")
                      val displayItem: String = displayObj.getString("item")
                      val displayItemType: String = displayObj.getString("item_type")
                      val posId: String = displayObj.getString("pos_id")
                      val order: String = displayObj.getString("order")

                      // Encapsulate as PageDisplayLog
                      val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)

                      // Send to the topic DWD_PAGE_DISPLAY_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                    }
                  }

                  // Extract event data
                  val actionsJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                  if (actionsJsonArr != null && actionsJsonArr.size() > 0) {
                    for (i <- 0 until actionsJsonArr.size()) {
                      val actionObj: JSONObject = actionsJsonArr.getJSONObject(i)
                      // Extract event fields
                      val actionId: String = actionObj.getString("action_id")
                      val actionItem: String = actionObj.getString("item")
                      val actionItemType: String = actionObj.getString("item_type")
                      val actionTs: Long = actionObj.getLong("ts")

                      // Encapsulate as PageActionLog
                      val pageActionLog: PageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)

                      // Send to the topic DWD_PAGE_ACTION_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                    }
                  }
                }

                // startup data
                val startObj: JSONObject = jsonObj.getJSONObject("start")
                if (startObj != null) {
                  val entry: String = startObj.getString("entry")
                  val loadingTime: Long = startObj.getLong("loading_time")
                  val openAdId: String = startObj.getString("open_ad_id")
                  val openAdMs: Long = startObj.getLong("open_ad_ms")
                  val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")

                  // Encapsulate as StartLog
                  val startLog: StartLog = StartLog(mid, ba, uid, ar, ch, isNew, md, os, vc, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)

                  // Send to the topic DWD_START_LOG_TOPIC
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                }
              }
            }
            // Inside foreachPartition: Executed on the executor side, once per partition per batch
            // Flush Kafka
            MyKafkaUtils.flush()
          }
        )

        // Inside foreachRDD, outside foreachPartition: Executed on the driver side, once per batch (periodically)
        // Committing offsets
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
