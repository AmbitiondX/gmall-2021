package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import com.ibm.icu.text.SimpleDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.util
import java.util.Date


object DauHandler {
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val midAndLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.logDate, log.mid), log)
    })
    val midAndLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()
    val midAndLogDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(ite => {
      ite.toList.sortWith(_.ts < _.ts).take(1)
    })
    val value: DStream[StartUpLog] = midAndLogDateToListLogDStream.flatMap(_._2)
    value
  }

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
  //   缺点 ：每个分区创建一个连接，还可以更精简一点
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.mapPartitions(part => {
      // 获取jedis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = part.filter(log => {
        // 比较当前log的mid是否存在于mids
        val redisKey: String = "DAU:" + log.logDate
        !jedis.sismember(redisKey,log.mid)
      })
      // 关闭jedis连接
      jedis.close()
      logs
    })
  }

  // 版本3
  //   缺点 ：
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc: SparkContext) = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    startUpLogDStream.transform(rdd => {
      // 获取jedis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      // 取出集合redis中的mid集合
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey: String = "DAU:" + date
      val mids: util.Set[String] = jedis.smembers(redisKey)
      // 广播变量，广播mids
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      val value: RDD[StartUpLog] = rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })
      // 关闭jedis连接
      jedis.close()
      value
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