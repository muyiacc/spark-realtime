package com.muzhe.gmall.realtime.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * @Author muyiacc
 * @Date 2023/6/6 006 17:06
 * @Description: TODO Kafka工具类，用于生产和消费
 */
object MyKafkaUtils {
  /**
   * 消费者配置
   */
  val consumerConfigs = mutable.Map[String,Object](
    // 集群位置
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    // kv反序列化器
    //ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    //ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_KEY_STRINGDESERIALIZER),

    //ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    //ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_VALUE_STRINGDESERIALIZER),

    // 消费者组id,由外部传入
  )

  /**
   * 基于sparkstreaming消费
   */
  def getKafkaDstream(ssc: StreamingContext, topic: String, groupid: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)

    val kafkaDstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), consumerConfigs)
    )
    kafkaDstream
  }

  /**
   * 基于sparkstreaming消费，使用指定的offset
   */
  def getKafkaDstream(ssc: StreamingContext, topic: String, groupId: String,offsets : Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), consumerConfigs, offsets)
    )
    kafkaDstream
  }

  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String,String] = createKafkaProducer()

  /**
   * 创建生产者对象
   */
  def createKafkaProducer(): KafkaProducer[String,String] = {
    // 生产者配置
    val producerConfigs = new util.HashMap[String, AnyRef]()
    // 集群连接
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))

    // kv序列化
    //producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    //producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,MyPropsUtils(MyConfig.KAFKA_KEY_STRINGSERIALIZER))

    //producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    //producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,MyPropsUtils(MyConfig.KAFKA_VALUE_STRINGSERIALIZER))

    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG,"all")
    // batch.size 默认 16kb
    // linger.ms 默认 0
    // retries 默认 0

    // 幂等性 默认true
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    val kafkaProducer = new KafkaProducer[String, String](producerConfigs)
    kafkaProducer
  }

  /**
   * 生产 按照默认的分区策略
   */
  def send(topic:String,msg:String): Unit = {
    producer.send(new ProducerRecord[String,String](topic,msg))
  }

  /**
   * 生产 按照key分区
   */
  def send(topic: String, key:String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭生产者对象
   */
  def close(): Unit = {
    if (producer !=null ) producer.close()
  }

  /**
   * 刷写，将缓冲区的数据刷写到磁盘
   */
  def flush(): Unit = {
    producer.flush()
  }

}
