package com.muzhe.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.muzhe.gmall.realtime.bean._
import com.muzhe.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
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
 * 3.1 转换数据结构
 * 专用结构  Bean
 * 通用结构 Map JsonObject
 * 3.2 分流
 *
 * 4 写出到 DWD 层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {

    // 1 准备实时环境
    val sparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2 从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG"
    val groupId: String = "ODS_BASE_LOG_GROUP"

    // TODO 从redis中读取offset, 指定offset消费
    val offsets = MyOffsetsUtils.readOffset(topicName, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 指定offset消费
      kafkaDStream =
        MyKafkaUtils.getKafkaDstream(ssc, topicName, groupId, offsets)
    } else {
      // 默认offset消费
      kafkaDStream =
        MyKafkaUtils.getKafkaDstream(ssc, topicName, groupId)
    }

    //    kafkaDStream.print(100)

    // TODO 提取offset,从当前的消费到的数据提取offset，但是不对流中的数据做处理，以便后续的操作
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 3 处理数据结构
    // 3.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      ConsumerRecord => {
        // 获取 ConsumerRecord 中的value, value就是数据
        val log = ConsumerRecord.value()
        // 转换成 Json对象
        val jsonObj = JSON.parseObject(log)
        // 返回
        jsonObj
      }
    )
    //    jsonObjDStream.print(100)

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

    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC" //页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" //启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" //错误数据

    // 分流规则
    //  页面数据：拆分成页面 访问，曝光，事件 分别发送到对应的topic
    //  启动数据：整条数据发送到对应的topic
    //  错误数据：只要包含错误字段，整条发送到对应的topic

    // 发送数据 - 使用行动算子
    jsonObjDStream.foreachRDD(
      rdd => {

        // 为了刷写缓冲区到磁盘，我们需使用foreachPartition算子，在分区发送完数据之后刷写
        rdd.foreachPartition(
          jsonIterator => {
            for (jsonObj <- jsonIterator) {
              // 分流过程
              // 分流错误数据
              val errObj = jsonObj.getJSONObject("err")
              if (errObj != null) {
                // 将错误数据发送到 DWD_ERROR_LOG_TOPIC
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, errObj.toJSONString)
              } else {
                // 提取公共字段
                val commonObj = jsonObj.getJSONObject("common")
                val ar = commonObj.getString("ar")
                val uid = commonObj.getString("uid")
                val os = commonObj.getString("os")
                val ch = commonObj.getString("ch")
                val isNew = commonObj.getString("is_new")
                val md = commonObj.getString("md")
                val mid = commonObj.getString("mid")
                val vc = commonObj.getString("vc")
                val ba = commonObj.getString("ba")
                // 提取时间戳
                val ts = jsonObj.getLong("ts")

                // 页面数据
                val pageObj = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  // 提取page字段
                  val pageId = pageObj.getString("page_id")
                  val pageItem = pageObj.getString("item")
                  val pageItemType = pageObj.getString("item_type")
                  val duringTime = pageObj.getLong("during_time")
                  val lastPageId = pageObj.getString("last_page_id")
                  val sourceType = pageObj.getString("source_type")

                  // 把页面数据封装成bean对象
                  val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId,
                    lastPageId, pageItem, pageItemType, sourceType, duringTime, ts)

                  // 发送到 DWD_PAGE_LOG_TOPIC
                  // 注意：JSON.toJSONString封装pageLog时，需要传入第二个参数，表示直接使用字段，否则将从get,set方法找，
                  // scala的样例类并没有get,set方法，故此采用此方法解决该问题
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                  // 因为曝光和事件都是在页面中才有可能提取到，所以在这里提取它们
                  // 提取曝光数据
                  val displaysJsonArr = jsonObj.getJSONArray("displays")
                  // 判断曝光是否有数据
                  if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                    for (i <- 0 until displaysJsonArr.size()) {
                      // 提取曝光字段
                      val displayObj = displaysJsonArr.getJSONObject(i)
                      val displayType = displayObj.getString("display_type")
                      val displayItem = displayObj.getString("item")
                      val displayItemType = displayObj.getString("item_type")
                      val displayPosId = displayObj.getLong("pos_id")
                      val displayOrder = displayObj.getLong("order")

                      // 封装成对象以便发送
                      val pageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId,
                        lastPageId, pageItem, pageItemType, sourceType, duringTime, displayType, displayItem,
                        displayItemType, displayOrder, displayPosId, ts)

                      // 发送数据到 DWD_PAGE_DISPLAY_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                    }
                  }

                  // 提取事件数据 操作类似 displays
                  val actionsJsonArr = jsonObj.getJSONArray("actions")
                  if (actionsJsonArr != null && actionsJsonArr.size() > 0) {
                    for (i <- 0 until actionsJsonArr.size()) {
                      val actionObj = actionsJsonArr.getJSONObject(i)
                      val actionItem = actionObj.getString("item")
                      val actionId = actionObj.getString("action_id")
                      val actionItemType = actionObj.getString("item_type")
                      //                    val actionTs = actionObj.getLong("ts")

                      // 封装成对象
                      val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId,
                        lastPageId, pageItem, pageItemType, sourceType, duringTime, actionId, actionItem,
                        actionItemType, ts)

                      // 发送数据到  DWD_PAGE_ACTION_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                    }
                  }
                }

                // 启动数据
                val startObj = jsonObj.getJSONObject("start")
                if (startObj != null && startObj.size() > 0) {
                  val entry = startObj.getString("entry")
                  val openAdSkipMs = startObj.getLong("open_ad_skip_ms")
                  val openAdMs = startObj.getLong("open_ad_ms")
                  val loadingTime = startObj.getLong("loading_time")
                  val openAdId = startObj.getLong("open_ad_id")

                  // 封装
                  val startLog = StartLog(uid, ar, ch, isNew, md, os, vc, entry, openAdId,
                    loadingTime, openAdMs, openAdSkipMs, ts)

                  // 发送到 DWD_START_LOG_TOPIC
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))

                }
              }
            }
            // foreachPartition里面： Executor端执行，每批次执行一次
            MyKafkaUtils.flush()
          }
        )

        /*
        rdd.foreach(
          jsonObj => {

            // 提交offset??? 在foreach里面，在 executor执行，每条数据执行一次
          }
        )
        */

        // 提交offset??? 在foreach外面，foreachRDD里面，在driver端执行,一批次执行一次，周期性执行
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    // 提交offset?? foreachRDD外面，主程序执行，只执行一次

    ssc.start()
    ssc.awaitTermination()

  }
}
