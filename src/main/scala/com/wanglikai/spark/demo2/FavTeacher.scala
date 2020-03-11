package com.wanglikai.spark.demo2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {

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
    //    subAndTea.foreach(println(_))
        val reduced: RDD[((String, String), Int)] = subAndTea.reduceByKey(_+_)
        val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    val res: RDD[(String, List[(String, Int)])] = grouped.mapValues(x => {
      val list: List[((String, String), Int)] = x.toList
      val sorded: List[((String, String), Int)] = list.sortBy(_._2).reverse
      val maped = sorded.map(s => {
        (s._1._2, s._2)
      })
      maped
    })
    res.foreach(println(_))

    sc.stop()
  }

}
