package com.wanglikai.spark.sqldemo3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpStatic {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IpStatic")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("C:\\sparkdata\\ip\\rule")
    val rulesRdd: RDD[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      (fields(2).toLong, fields(3).toLong, fields(6))
    })

    //收集到Driver端
    val rules: Array[(Long, Long, String)] = rulesRdd.collect()
    //把ip规则广播到各个executor中
    val broadCastRules: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    val accessLog: RDD[String] = sc.textFile("C:\\sparkdata\\ip\\input")

    val provinces: RDD[(String, Int)] = accessLog.map(line => {
      val fields: Array[String] = line.split("[|]")
      val ip: String = fields(1)
      val ipNum: Long = IpUtil.ip2Long(ip)
      val ipR: Array[(Long, Long, String)] = broadCastRules.value
      val province: String = IpUtil.searchIp(ipR, ipNum)
      (province, 1)
    })

    val reduced: RDD[(String, Int)] = provinces.reduceByKey(_+_)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)

    sorted.saveAsTextFile("C:\\yarnData\\ip\\output")

    sc.stop()

  }

}
