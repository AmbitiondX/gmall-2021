package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization


import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreaminhgContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka数据
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

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


    val idToDetailDStream: DStream[(String, OrderDetail)] = orderDetaiDStream.mapPartitions(iter => {
      iter.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)
      })
    })

    //5.使用fullouterjoin防止join不上的数据丢失
    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream.fullOuterJoin(idToDetailDStream)

    val noUserDStream: DStream[SaleDetail] = fullDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      // 创建一个list集合用来存放结果数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102")
      partition.foreach {
        case (orderId, (infoOpt, detailOpt)) => {
          // 设计rediskey
          val orderInfoKey: String = "orderInfo" + orderId
          val orderDetailKey: String = "orderDetail" + orderId
          //判断订单表是否
          // a.订单存在
          // a.1获取订单表数据
          if (infoOpt.isDefined) {
            val orderInfo: OrderInfo = infoOpt.get
            // 判断详情表是否存在
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              details.add(new SaleDetail(orderInfo, orderDetail))
            }
            // b.将orderInfo数据写入redis连接

            val orderInfoJson: String = Serialization.write(orderInfo)

            jedis.set(orderInfoKey, orderInfoJson)
            jedis.expire(orderInfoKey, 30)

            // 查询orderDetail的缓存
            if (jedis.exists(orderDetailKey)) {
              val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
              val set: Set[String] = orderDetailSet.asScala.toSet
              for (elem <- set) {
                val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                details.add(new SaleDetail(orderInfo, orderDetail))
              }
            }
          } else {
            // 判断detailOpt是否存在
            if (detailOpt.isDefined) {

              val orderDetail: OrderDetail = detailOpt.get
              //  val orderInfoKey: String = "orderInfo" + orderDetail.order_id
              // 查询orderInfo的缓存
              // 存在对应的orderInfo，则添加进detail
              if (jedis.exists(orderInfoKey)) {
                val orderInfoStr: String = jedis.get(orderInfoKey)
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                details.add(new SaleDetail(orderInfo, orderDetail))
              } else {
                //不存在则缓存orderDetail,并设置过期时间
                val orderDetailStr: String = Serialization.write(orderDetail)
                jedis.sadd(orderDetailKey, orderDetailStr)
                jedis.expire(orderDetailKey, 100)
              }
            }
          }
        }
      }
      jedis.close()
      details.asScala.toIterator
    })

    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(partition => {
      // a.获取jedis连接
      val jedis: Jedis = new Jedis("hadoop102")
      // b.查库
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        val userInfoKey: String = "userInfo" + saleDetail.user_id
        val userInfoJson: String = jedis.get(userInfoKey)
        // 将数据转化为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })

    // 将数据写入ES
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}