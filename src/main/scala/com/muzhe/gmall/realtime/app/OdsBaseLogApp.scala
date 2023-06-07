package com.muzhe.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.muzhe.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/6 006 18:26
 * @Description: TODO 日志数据消费分流
 */

/**
 * 日志数据消费分流
 *
 * 1 准备环境 StreamingContext
 *
 * 2 从kafka消费数据
 *
 * 3 处理数据
 *  3.1 转换数据结构
 *        专用结构  Bean
 *        通用结构 Map JsonObject
 *  3.2 分流
 *
 * 4 写出到 DWD 层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {

    // 1 准备实时环境
    val sparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2 从kafka中消费数据
    val topicName:String = "ODS_BASE_LOG"
    val groupid:String = "ODS_BASE_LOG_GROUP"
    val kafkaDstream:  InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDstream(ssc, topicName, groupid)

//    kafkaDstream.print(100)

    // 3 处理数据结构
    // 3.1 转换数据结构
    val jsonObjDStream = kafkaDstream.map(
      ConsumerRecord => {
        // 获取 ConsumerRecord 中的value, value就是数据
        val log = ConsumerRecord.value()
        // 转换成 Json对象
        val jsonObj = JSON.parseObject(log)
        // 返回
        jsonObj
      }
    )
    jsonObjDStream.print(100)

    // 3.2 分流
    //  日志数据
    //    页面访问数据
    //      公共字段
    //      页面数据
    //      曝光数据
    //      事件数据
    //      错误数据
    //    启动数据
    //      公共字段
    //      启动数据

    val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC" //页面事件
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC" //启动数据
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC" //错误数据

    // 分流规则
    //  页面数据


    ssc.start()
    ssc.awaitTermination()

  }
}
