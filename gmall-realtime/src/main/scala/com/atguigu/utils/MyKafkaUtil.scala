package com.atguigu.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.io.{FileInputStream, InputStreamReader}
import java.{io, lang}
import java.util.Properties
import scala.collection.mutable

object MyKafkaUtil {
  //1.使用自定义工具类，创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //2.用于初始化链接到集群的地址
  val broker_list: String = properties.getProperty("kafka.broker.list")

  //3.kafka消费者配置

  var kafkaParam = mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.GROUP_ID_CONFIG -> "bigdata2021",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )

  def getKafkaStream(topic: String, ssc: StreamingContext) ={
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam)
    )
  }


}
