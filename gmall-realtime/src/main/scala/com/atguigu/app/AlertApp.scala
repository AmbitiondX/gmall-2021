package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    //4.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->mid  v->log
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(part => {
      part.map(record => {
        val recordValue: String = record.value()
        val log: EventLog = JSON.parseObject(recordValue, classOf[EventLog])
        val date: String = sdf.format(new Date(log.ts))
        log.logDate = date.split(" ")(0)
        log.logHour = date.split(" ")(1)
        (log.mid, log)
      })
    })

    //5.开窗5min
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.分组聚合按照mid
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //7.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(part => {
      part.map {
        case (mid, iter) => {
          println("11111111111111111111111111")
          // 创建set集合用来保存uid
          val uids: util.HashSet[String] = new util.HashSet[String]()
          // 创建set集合用来保存优惠券所涉及商品id
          val itemIds: util.HashSet[String] = new util.HashSet[String]()
          // 创建List集合用来保存用户行为事件
          val events: util.ArrayList[String] = new util.ArrayList[String]()

          //标志位
          var bool = true

          //判断有没有浏览商品行为
          breakable {
            iter.foreach(log => {
              events.add(log.evid)

              if (log.evid.equals("clickItem")) { //判断用户是否有浏览商品行为
                bool = false
                break()
              } else if (log.evid.equals("coupon")) { //判断用户是否有领取购物券行为
                itemIds.add(log.itemid)
                uids.add(log.uid)
              }
            })
          }
          //产生疑似预警日志
          println("222222222222222222222222222222222222222")
          ((uids.size() >= 3 && bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
        }
      }
    })

    // 8.生成预警日志(将数据保存至CouponAlertInfo样例类中，文档中有)，条件：符合第七步要求，并且uid个数>=3（主要为“过滤”出这些数据），实质：补全CouponAlertInfo样例类
    val alterDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)

    // 9.将预警数据写入ES
    alterDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        println("3333333333333333333333333333333333")
        val indexName: String = GmallConstants.ES_ALERT_INDEXNAME + "-" + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alert => {
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
