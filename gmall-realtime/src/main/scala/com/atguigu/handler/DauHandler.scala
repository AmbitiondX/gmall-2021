package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object DauHandler {
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    // 转化为二元组，好做groupbykey
    val tupleDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.logDate, log.mid), log)
    })
    // 聚合
    val groupDStream: DStream[((String, String), Iterable[StartUpLog])] = tupleDStream.groupByKey()

    // 按照时间升序，取第一个
    val takeOneDStream: DStream[((String, String), List[StartUpLog])] = groupDStream.mapValues(ite => {
      ite.toList.sortWith(_.ts < _.ts).take(1)
    })

    // 打散
    takeOneDStream.flatMap(_._2)
  }

  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.mapPartitions(ite => {
      // 获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = ite.filter(log => {
        // 创建rediskey
        val redisKey: String = "DAU:" + log.logDate
        !jedis.sismember(redisKey, log.mid)
      })
      // 关闭redis连接
      jedis.close()
      logs
    })
  }


  /**
   * 将mid保存到redis
   * @param startUpLogDStream
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        // 获取redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        part.foreach(log => {
          // 创建rediskey
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        // 关闭redis连接
        jedis.close()
      })
    })
  }

}