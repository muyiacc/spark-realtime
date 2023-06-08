package com.muzhe.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util
import scala.collection.mutable

/**
 * @Author muyiacc
 * @Date 2023/6/7 007 19:10
 */

/**
 * Offset管理工具类，用于往redis中存储和读取offset
 *
 * 管理方案：
 *  1 后置提交偏移量  ---> 手动控制偏移量提交
 *  2
 *  3 手动提取偏移量到redis
 *    1) 从kafka消费到数据后，获取偏移量
 *    2) 等数据成功写出后，将偏移量写到redis
 *    3) 在kafk消费之前，读取redis中存储的偏移量，使用读取到的偏移量来从kafka中消费数据
 */
object MyOffsetsUtils {

  /**
   * 往redis中存储偏移量
   *
   * 几个问题：
   *  offset从哪来？
   *    从消费到的数据中提取offset
   *    offsetRanges : Array[OffsetRange]
   *  offset的结构？
   *    kafka中offset的结构
   *      groupId + topic + partition ==> offset
   *  redis中怎么存储offset?
   *    类型: hash
   *    key: groupId + topic
   *    value: partition - offset , partition - offset
   *    写入api: hset/hmset
   *    读取api: hgetall
   *    是否过期: 过期
   */

  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0){
      val offsets = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition = offsetRange.partition
        val endOffset = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      println("提交offset: " + offsets)
      // redis存入数据
      val jedis = MyRedisUtils.getJedisFromPool()
      val redisKey = s"offsets:$topic$groupId"
      jedis.hset(redisKey, offsets)
      jedis.close()
    }
  }


  /**
   * 从redis中读取偏移量
   *
   * 考虑SparkStreaming要求的offset格式
   *
   */

  def readOffset(topic:String, groupId: String): Map[TopicPartition, Long] = {
    val jedis = MyRedisUtils.getJedisFromPool()

    val redisKey = s"offsets:$topic$groupId"
    val offsets = jedis.hgetAll(redisKey)
    println("读取到offset: " + offsets)
    val results = mutable.Map[TopicPartition, Long]()

    // 将java的map转换成scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val tp = new TopicPartition(topic, partition.toInt)
      results.put(tp, offset.toLong)
    }

    jedis.close()

    results.toMap
  }



}
