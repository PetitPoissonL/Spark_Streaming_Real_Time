package com.bigdata.gmall.realtime.util

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * Kafka utility class used for producing and consuming data。
 */
object MyKafkaUtils {

  /**
   * Consumer configuration
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // Kafka cluster location
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    // Key-Value deserializer
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupID
    // Offset commit
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // Auto-commit interval
    // ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    // Offset reset
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * Consuming data based on Spark Streaming, obtaining a Kafka DStream, using the default offset
   */
  def getKafkaDStream(ssc:StreamingContext, topic:String, groupId:String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   * Producer object
   */
  val producer : KafkaProducer[String, String] = createProducer()

  /**
   * Create a producer object
   */
  def createProducer():KafkaProducer[String, String] = {
    val producerConfigs = new util.HashMap[String, AnyRef]
    // Producer configuration: producerConfigs
    // Kafka cluster location
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    // Key-Value serializer
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    // batch.size default: 16kb
    // linger.ms default: 0
    // retries default: 0
    // Idempotence configuration
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * Produce (Using the default sticky partitioning strategy)
   */
  def send(topic : String, msg : String):Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * Produce (Partitioning by key)
   */
  def send(topic: String, key : String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * Close the producer
   */
  def close():Unit = {
    if(producer != null) producer.close()
  }
}
