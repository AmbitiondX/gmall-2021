package com.atguigu.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties


object MyKafkaUtil {

  private val prop: Properties = PropertiesUtil.load("config.properties")
  private val broker_list: String = prop.getProperty("kafka.broker.list")
  var kafkaParams = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall20210526",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  def getKafkaStream(topic: String,ssc:StreamingContext) = {
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Array(topic), kafkaParams)
    )
    kafkaDStream
  }


}
