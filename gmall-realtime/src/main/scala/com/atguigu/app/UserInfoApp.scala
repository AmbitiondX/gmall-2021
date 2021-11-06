package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.分别消费kafka中订单表的数据以及订单明细表的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //4.将用户表的数据转化为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        JSON.parseObject(record.value(), classOf[UserInfo])
      })
    })

    // 测试
//    userInfoDStream.print()

    // 将用户数据缓存到redis
    // 生产环境中，用户数据比较大，一般都存储在hbase中
    userInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 获取redis连接
        val jedis: Jedis = new Jedis("hadoop102")
        partition.foreach(userInfo => {
          // 设计rediskey
          val userInfoKey: String = "userInfoKey:" + userInfo.id
          jedis.set(userInfoKey,)
        })
      })
    })

    // 开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
