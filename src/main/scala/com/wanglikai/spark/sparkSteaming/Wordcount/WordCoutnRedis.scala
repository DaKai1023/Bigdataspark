package com.wanglikai.spark.sparkSteaming.Wordcount

import com.wanglikai.redisutil.JedisConnectionPool
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object WordCoutnRedis {

  def main(args: Array[String]): Unit = {

   val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
    val ssc = new StreamingContext(conf,Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest", //latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("sparkwc")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)
    )
//    val stream: DStream[String] = text.map(_.value())
//
//    val wordAndCount: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    stream.foreachRDD(kafkaRDD =>{
      if(!kafkaRDD.isEmpty()) {
        //取出kafka偏移量
        val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //做自己的业务逻辑
        val lines: RDD[String] = kafkaRDD.map(_.value())
        val res: RDD[(String, Int)] = lines.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, false)
//        触发job运行 foreachPartition是action方法
        res.foreachPartition(it => {
          //获取连接
          val conn: Jedis = JedisConnectionPool.getConnection
          //把单词更新到redis
          it.foreach(x=>{
            conn.incrBy(x._1,x._2)
          })
          conn.close()
          it
        })

        //自己的业务逻辑结束

        //打印kafka偏移量
        println("================kafka偏移量打印=======================")
        offsetRanges.foreach(x => {
          println(s"kafkapartition=${x.partition}  kafkapartitionoffsets=${x.fromOffset}")
        })
        println("======================================================")

        //手动更新kafka偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
