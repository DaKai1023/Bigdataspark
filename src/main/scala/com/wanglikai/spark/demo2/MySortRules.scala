package com.wanglikai.spark.demo2

object MySortRules {

  implicit val teacherOrdering = new Ordering[((String,String),Int)] {
    override def compare(x: ((String, String), Int), y: ((String, String), Int)): Int = y._2 - x._2
  }

}
