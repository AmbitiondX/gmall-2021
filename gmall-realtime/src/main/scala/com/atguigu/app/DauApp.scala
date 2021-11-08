package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import org.apache.phoenix.spark._


object DauApp {
  def main(args: Array[String]): Unit = {
    // 设置sparkconf文件
    val conf: SparkConf = new SparkConf().setAppName("gmall2021").setMaster("local[*]")
    // 创建streamingcontext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 获取kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    // 将json格式转换为样例类
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(part => {
      part.map(record => {
        val log: String = record.value()
        val caseLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
        val time: String = sdf.format(caseLog.ts)
        caseLog.logDate = time.split(" ")(0)
        caseLog.logHour = time.split(" ")(1)
        caseLog
      })
    })

    // 分区间过滤
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)


    // 分区内过滤
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    //数据测试
    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()
    filterByGroupDStream.count().print()

    // 将数据(mid)保存到redis
    DauHandler.saveToRedis(startUpLogDStream)

    // 将数据(完整数据)保存到hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 测试kafka连接
//    kafkaDStream.foreachRDD(rdd => {
//      rdd.foreach(record => {
//        println(record.value())
//      })
//    })
//    startUpLogDStream.foreachRDD(rdd => rdd.foreach(println))


    //启动ssc并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
