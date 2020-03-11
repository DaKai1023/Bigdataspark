package com.wanglikai.spark.sqldemo3

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FavTeacherSQL {

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

    df.createTempView("t_sub_teacher")

    val tem_v1: DataFrame = spark.sql("select subject,teacher,count(*) as cnts from t_sub_teacher group by subject,teacher")

    tem_v1.createTempView("tem_v1")

    val tem_v2 = spark.sql("select subject,teacher,cnts,row_number() over(partition by subject order by cnts desc) as rk from tem_v1")

    tem_v2.createTempView("tem_v2")
    val result = spark.sql("select * from tem_v2 where rk <= 2")

    result.cache()
    result.show()

    result.write.json("C:\\sparkdata\\favt\\output\\res.json")

    spark.stop()
  }

}
