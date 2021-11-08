package com.atguigu.app


import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreaminhgContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    //3.获取kafka数据
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetaiDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转化为样例类
    val idToInfoDStream: DStream[(String, OrderInfo)] = orderDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转化为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //create_time yyyy-MM-dd HH:mm:ss
        //补全create_date字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        //补全create_hour字段
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        //返回数据
        (orderInfo.id, orderInfo)
      })
    })

//    idToInfoDStream.print()


    val idToDetailDStream: DStream[(String, OrderDetail)] = orderDetaiDStream.mapPartitions(iter => {
      iter.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)
      })
    })

//    idToDetailDStream.print()

    //5.使用fullouterjoin防止join不上的数据丢失
    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream.fullOuterJoin(idToDetailDStream)

    fullDStream.print()

    //6.采用加缓存的方式处理 因网络延迟带来的数据丢失问题
    val noUserInfoDStream: DStream[SaleDetail] = fullDStream.mapPartitions(parititon => {
      implicit val formats = org.json4s.DefaultFormats
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      // 获取redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)

      parititon.foreach {
        case (orderId, (infoOpt, detailOpt)) => {
          // 设计redisKey
          // orderInfo
          val orderInfoRedisKey: String = "orderInfo" + orderId
          val orderDetailRedisKey: String = "orderDetail" + orderId

          // 判断infoOpt是否存在
          // 存在的情况：
          if (infoOpt.isDefined) {
            val orderInfo: OrderInfo = infoOpt.get
            // 判断detailOpt(订单详情)是否存在
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              // 将join上的订单和详情写入数组当中
              details.add(new SaleDetail(orderInfo, orderDetail))
            }
            // 将orderInfo写入redis缓存
            val orderInfoJson: String = Serialization.write(orderInfo)
            jedis.set(orderInfoRedisKey, orderInfoJson)
            // ***给存入的redis数据设置过期时间
            jedis.expire(orderInfoRedisKey, 30)

            // 查询orderDetail缓存
            if (jedis.exists(orderDetailRedisKey)) {
              val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)
              for (elem <- orderDetailSet.asScala) {
                // 将字符串转化为样例类
                val elemDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                details.add(new SaleDetail(orderInfo, elemDetail))
              }
            }
          } else {
            // 到此表明orderInfo数据不存在
            // 如果orderDetail数据存在
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              // 去查询orderInfo缓存
              if (jedis.exists(orderInfoRedisKey)) {
                // 去除redis数据，并转化为样例类
                val orderInfoJSON: String = jedis.get(orderInfoRedisKey)
                details.add(new SaleDetail(JSON.parseObject(orderInfoJSON, classOf[OrderInfo]), orderDetail))
              } else {
                // orderInfo缓存没有对应上的数据，则缓存自己
                val orderDetailJSON: String = Serialization.write(orderDetail)
                jedis.sadd(orderDetailRedisKey, orderDetailJSON)
                // 对orderDetail数据设置过期时间
                jedis.expire(orderDetailRedisKey, 30)
              }
            }
          }
        }
      }
      // 关闭redis连接
      jedis.close()
      details.asScala.toIterator
    })

    // 测试
//    noUserInfoDStream.print()

    // 关联UserInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserInfoDStream.mapPartitions(partition => {
      // 获取redis连接
      val jedis: Jedis = new Jedis("hadoop102")
      val iterator: Iterator[SaleDetail] = partition.map(saleDetail => {
        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id
        val userInfoJSON: String = jedis.get(userInfoRedisKey)
        val userInfo: UserInfo = JSON.parseObject(userInfoJSON, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      iterator
    })

//    saleDetailDStream.print()

    // 将数据保存到ES
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME + sdf.format(System.currentTimeMillis())
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
