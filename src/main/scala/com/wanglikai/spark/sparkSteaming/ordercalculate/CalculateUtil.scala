package com.wanglikai.spark.sparkSteaming.ordercalculate

import java.util.Calendar

import com.wanglikai.redisutil.{JedisClusterConnectionPool, JudgeIp, Judgelp}
import com.wanglikai.spark.sqldemo3.IpUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.JedisCluster

object CalculateUtil {
//计算总成交金额
  def calculateSum(orders :RDD[(String, Long, String, String, Double, Float, Double)])={
    //reduce是action的算子，触发job,统计出批次所有订单累加的金额,sum是返回Driver端的结果
    val sum: Double = orders.map(_._7).reduce(_+_)
    //这个是在Driver端打开的redis连接，sum是Driver端的数据，是在Driver端做累加
    val conn = JedisClusterConnectionPool.getConnection
    //累加到redis中
    conn.incrByFloat("orderSum",sum)

  }
  //按照商品类别统计
  def  calculateByItem(orders :RDD[(String, Long, String, String, Double, Float, Double)]): Unit ={

    val iteAndMoney = orders.map(x=>(x._3,x._7))
    val reduced: RDD[(String, Double)] = iteAndMoney.reduceByKey(_+_)
    //触发action，在redis中累加
    reduced.foreachPartition(it =>{
      //在executor中打开redis连接，累加到redis中
      val conn: JedisCluster = JedisClusterConnectionPool.getConnection
      //在executor中执行的scala代码
      it.foreach(x=>{
        conn.incrByFloat(x._1,x._2)
      })

    })

  }
//统计每个省份总的销售额
  def calculateByZone(orders :RDD[(String, Long, String, String, Double, Float, Double)],rulesRef :Broadcast[Array[(Long, Long, String)]])={

    val provinceAndMoney: RDD[(String, Double)] = orders.map(x => {
      val rules = rulesRef.value
      val province: String = IpUtil.searchIp(rules, x._2)
      (province, x._7)
    })
    val reduced: RDD[(String, Double)] = provinceAndMoney.reduceByKey(_+_)

    reduced.foreachPartition(it=>{
      val conn = JedisClusterConnectionPool.getConnection
      //在executor中执行scala代码
      it.foreach(x=>{
        conn.incrByFloat(x._1,x._2)
      })
    })


  }

  def realTimeEtl(lines: RDD[String]) = {
    val arrRdd: RDD[Array[String]] = lines.map(line => {
      val arr = line.split(" ")
      arr
    })
    val filtered: RDD[Array[String]] = arrRdd.filter(arr => {
      var flag = false
      try{
        //判断是不是正好包含个字段,判断第二个字段是不是ip
        if(arr.length == 6 && Judgelp.ipCheck(arr(1))){
          //判断price是不是能转换成Double
          arr(4).toDouble
          //判断amount是不是能转换成float
          arr(5).toFloat
          flag = true
        } else {
          println("arr is not six fields")
        }
      } catch {
        case _: Exception => {
          println("data is err format")
        }
      }
      flag
    })

    val oreders: RDD[(String, Long, String, String, Double, Float, Double)] = filtered.map(arr => {
      val ipNum: Long = IpUtil.ip2Long(arr(1))
      val price = arr(4).toDouble
      val amount = arr(5).toFloat
      //用户名,ip,商品类别,商品名称,单价,购买个数,订单总金额
      (arr(0), ipNum, arr(2), arr(3), price, amount, price * amount)
    })

    oreders
  }

  def saveToHdfs(orders: RDD[(String, Long, String, String, Double, Float, Double)]) = {
    //获取当前时间
    val c = Calendar.getInstance()
    val year = c.get(Calendar.YEAR)
    val month = c.get(Calendar.MONTH)
    val day = c.get(Calendar.DAY_OF_MONTH)
    val hour = c.get(Calendar.HOUR_OF_DAY)
    orders
      .coalesce(1)  //减少分区，以减少输出到hdfs中的文件数量
      .saveAsTextFile("hdfs://node4:8020/order1705e/"+year+"/"+(month+1)+"/"+day+"/"+hour+"/"+c.getTimeInMillis)

  }

}
