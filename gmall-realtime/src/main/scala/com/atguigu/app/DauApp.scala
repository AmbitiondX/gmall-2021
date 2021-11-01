package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstants
import com.atguigu.bean.StartUpLog
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)


    //4.将数据转换成样例类
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(part => {
      part.map(record => {
        val str: String = record.value()
        val log: StartUpLog = JSON.parseObject(str, classOf[StartUpLog])
        val date: String = sdf.format(log.ts)
        log.logDate = date.split(" ")(0)
        log.logHour = date.split(" ")(1)
        log
      })
    })

    // 添加缓存
    startUpLogDStream.cache()

    //4.分区间过滤
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)
//    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    // 验证是否过滤掉数据
    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()

    //5.分区内过滤
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.count().print()

    //6.将mid保存到redis
    DauHandler.saveToRedis(filterByGroupDStream)

    //7.将数据保存到hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 打印kafka数据
//    kafkaDStream.foreachRDD(rdd => {
//      rdd.foreach(record => {
//        println(record.value())
//      })
//    })

    //开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
