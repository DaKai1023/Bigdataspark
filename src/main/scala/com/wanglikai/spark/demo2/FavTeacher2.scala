package com.wanglikai.spark.demo2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FavTeacher2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo2a").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lines  = sc.textFile("C:\\sparkdata\\favt\\input")
    val subAndTea: RDD[((String, String), Int)] = lines.map(x => {
      val arr: Array[String] = x.split("[/]")
      val arrSub = arr(2).split("[.]")
      val subject: String = arrSub(0)
      val teacher: String = arr(3)

      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subAndTea.reduceByKey(_+_)

    reduced.persist()
    reduced.cache()

    val subjects: Array[String] = reduced.map(x=>x._1._1).distinct().collect()

    for (subject <- subjects){
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1.equals(subject))
      val sorted: RDD[((String, String), Int)] = filtered.sortBy(_._2,false)
      val res: RDD[(String, Int)] = sorted.map(x=>(x._1._2,x._2))
      res.saveAsTextFile("C:\\sparkdata\\favt\\output\\"+subject+"out")

    }
    sc.stop()
  }


}
