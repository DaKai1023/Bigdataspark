package com.wanglikai.spark.sparkSteaming.ordercalculate

import com.wanglikai.spark.sqldemo3.IpUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.JedisCluster

object OrderCalculate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("calaulate").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Duration(10000))
    //读规则表
    val rulesRdd: RDD[(Long, Long, String)] = ssc.sparkContext.textFile("C:\\sparkdata\\ip\\rule")
      .map(x => {
        val arr: Array[String] = x.split("[|]")
        (arr(2).toLong, arr(3).toLong, arr(6))

      })
    rulesRdd
    //收集到Driver端
    val rules = rulesRdd.collect()
    //广播rules到各个executor中
    val rulesRef: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(rules)

    if(args.length<3){
      System.err.println(s"""
          |Usage: DirectKafkaWordCount <brokers> <groupId> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <groupId> is a consumer group name to consume from topics
          |  <topics> is a list of one or more kafka topics to consume from
          |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, groupId, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false : java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    messages.foreachRDD(kafkaRdd=>{
      if(!kafkaRdd.isEmpty()){
        val offsetRanges = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //写自己的业务

        //取出kafkaRdd中的Value
        val lines: RDD[String] = kafkaRdd.map(_.value())

        //实时etl，做数据清洗
        val orders: RDD[(String, Long, String, String, Double, Float, Double)] = CalculateUtil.realTimeEtl(lines)

        //======etl后的数据落到hdfs盘上========
        CalculateUtil.saveToHdfs(orders)

        //计算业务指标
        if(!orders.isEmpty()){
          CalculateUtil.calculateSum(orders)
          CalculateUtil.calculateByItem(orders)
          CalculateUtil.calculateByZone(orders,rulesRef)
        }
        //业务逻辑完成

        //提交kafka偏移量
        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
