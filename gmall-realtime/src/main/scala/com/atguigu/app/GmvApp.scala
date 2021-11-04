package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object GmvApp {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    // 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 获取Kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    // 封装成样例类，并补全字段
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(part => {
      part.map(record => {
        val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        info.create_date = info.create_time.split(" ")(0)
        info.create_hour = info.create_time.split(" ")(1).split(":")(0)
        info
      })
    })

    // 数据写入hbase
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 启动ssc，并阻塞线程
    ssc.start()
    ssc.awaitTermination()
  }
}
