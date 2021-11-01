package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.util


object DauHandler {
  // 版本1
  //   缺点 ：每一条数据都需要创建一个连接，浪费资源
  /*def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.filter(log => {
      // 获取jedis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      // 取出集合redis中的mid集合
      // 设计redis key
      val redisKey: String = "DAU:" + log.logDate
      val mids: util.Set[String] = jedis.smembers(redisKey)
      // 比较当前log的mid是否存在于mids
      val bool: Boolean = mids.contains(log.mid)
      // 关闭jedis连接
      jedis.close()
      // 存在则需要过滤掉，
      !bool
    })
  }*/

  // 版本2
  //   缺点 ：
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.mapPartitions(part => {
      // 获取jedis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      part.filter(log =>{
        // 取出集合redis中的mid集合
        val redisKey: String = "DAU:" + log.logDate
        val mids: util.Set[String] = jedis.smembers(redisKey)
        // 比较当前log的mid是否存在于mids
        val bool: Boolean = mids.contains(log.mid)
        !bool
      })
    })
  }


  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(ite => {
        // 获取jedis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        ite.foreach(log => {
          // 设计redis key
          val redisKey: String = "DAU:" + log.logDate
          // 把mid存入redis
          jedis.sadd(redisKey,log.mid)
        })
        // 关闭jedis连接
        jedis.close()
      })
    })
  }


}