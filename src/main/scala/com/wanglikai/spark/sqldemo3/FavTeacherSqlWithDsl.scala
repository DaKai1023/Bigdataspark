package com.wanglikai.spark.sqldemo3

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FavTeacherSqlWithDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("favteacher").master("local[*]").getOrCreate()
    import spark.implicits._
    val lines: Dataset[String] = spark.read.textFile("C:\\sparkdata\\favt\\input")

    val subjectAndTeacher: Dataset[(String, String)] = lines.map(x => {
      val arr: Array[String] = x.split("[/]")
      val arrSub = arr(2).split("[.]")
      val subject: String = arrSub(0)
      val teacher: String = arr(3)
      (subject, teacher)
    })
    val df: DataFrame = subjectAndTeacher.toDF("subject","teacher")

    import org.apache.spark.sql.functions._

    val res = df.groupBy($"subject", $"teacher")
      .agg(count("*") as ("cnts"))
      .select($"subject", $"teacher", $"cnts", row_number() over (Window.partitionBy($"subject") orderBy ($"cnts" desc)) as "rk")
      .select("*").where($"rk" <= 2)
    res.show()

    spark.stop()

  }

}
