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

//    midToLogDStream.print()

    //5.开窗5min
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.分组聚合按照mid
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(iter => {
      iter.map {
        case (mid, iter) => {
          val uids: util.HashSet[String] = new util.HashSet[String]()
          val itemIds: util.HashSet[String] = new util.HashSet[String]()
          val events: util.ArrayList[String] = new util.ArrayList[String]()
          var bool = true
          iter.foreach(log => {
            events.add(log.evid)
            breakable {
              if ("clickItem".equals(log.evid)) {
                bool = false
                break()
              } else if ("coupon".equals(log.evid)) {
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }
            }
          })
          ((uids.size() >= 3 && bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
        }
      }
    })

    //8.生成预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()
    //9.将预警日志写入ES并去重
    couponAlertInfoDStream.foreachRDD(part => {
      part.foreachPartition(part => {
        val list: List[(String, CouponAlertInfo)] = part.toList.map(log => {
          //设备id + 时间(精确到'分') 作为插入es的id，利用幂等性去重
          (log.mid + log.ts / 1000 / 60, log)
        })
        val times: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val indexName: String = GmallConstants.ES_ALERT_INDEXNAME + "-" + times
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
