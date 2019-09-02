package com.suixin

import org.apache.spark.sql._


object DataSet {
  case class Person(name: String,age:Int)

  def main(args: Array[String]): Unit = {

    val  sparkSession=SparkSession.builder()
      .master("local")
      .appName("DataSet")
      .getOrCreate()
    var sc=sparkSession.sparkContext
        //RDD
        val rdd=sc.parallelize(Seq(("tom",23),("sam",321)),2)

       //DF
       val idAgeRDDRow = sc.parallelize(Seq(Person("sdsad", 30), Person("dsad", 29), Person("dasd", 21)))
    //导入隐式转换
    import sparkSession.implicits._

            val df=idAgeRDDRow.toDF()
            df.show()
            println(df.schema)
            df.select("name").show()

    // Ds
    import sparkSession.implicits._
        val ds=Seq(Person("tom",23),Person("sam",321))
        val seq = Seq(Person("lily",33),Person("jack",22))
    sparkSession.createDataset(seq).show()
  }
}