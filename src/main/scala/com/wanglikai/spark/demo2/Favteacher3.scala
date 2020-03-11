package com.wanglikai.spark.demo2

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Favteacher3 {

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
    val subjects = subAndTea.map(_._1._1).distinct().collect()

    val reduced = subAndTea.reduceByKey(new SubjectPartitioner(subjects),_+_)
    import MySortRules.teacherOrdering
    val res = reduced.mapPartitions(it => {
      val treeSet = new mutable.TreeSet[((String, String), Int)]()
      it.foreach(x => {
        treeSet.add(x)
        if (treeSet.size == 3) {
          val last = treeSet.last
          treeSet.remove(last)

        }
      })
      treeSet.toIterator
    })
    res.saveAsTextFile("C:\\sparkdata\\favt\\output\\")

  }

}

class SubjectPartitioner(subjects :Array[String]) extends Partitioner{

  val partitionRules = new mutable.HashMap[String,Int]()
  var i = 0
  for (subject <- subjects){
      partitionRules.put(subject,i)
    i+=1
  }

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
      val tuple = key.asInstanceOf[(String,String)]

      partitionRules(tuple._1)

  }
}
