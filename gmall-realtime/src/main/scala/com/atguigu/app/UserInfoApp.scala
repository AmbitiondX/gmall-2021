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
import org.json4s.native.Serialization

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.分别消费kafka中订单表的数据以及订单明细表的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)



    // 将用户数据缓存到redis
    // 生产环境中，用户数据比较大，一般都存储在hbase中
    kafkaDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 获取redis连接
        val jedis: Jedis = new Jedis("hadoop102")
        partition.foreach(record => {
          // 转化为样例类，获取userid
          val userId: String = JSON.parseObject(record.value(), classOf[UserInfo]).id
          // 设计rediskey
          val userInfoKey: String = "userInfo:" + userId
          jedis.set(userInfoKey,record.value())
        })
        // 关闭jedis连接
        jedis.close()
      })
    })

    // 开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
