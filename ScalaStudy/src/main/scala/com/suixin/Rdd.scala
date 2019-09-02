package com.suixin

import org.apache.spark.sql.SparkSession

object Rdd {
  def main(args: Array[String]): Unit = {

    val ss=SparkSession.builder()
      .appName("RddStudy")
      .master("local")
//      .enableHiveSupport()
      .getOrCreate()
    val sc=ss.sparkContext
    val rdd=sc.parallelize(List(1,23,2,32,13),2)
    val result=rdd.map(a=>(a,1)).collect()
    result.foreach(println)

    sc.textFile("D://logs/applog/log1.txt")
      .map(data=>{

      })

//    val df:DataFrame=ss.sql("select")
//    df.cache()
//    df.select("dsd","dsadsa").collect().foreach({
//      t=>{println(t.getAs("dsad"))}
//    })
    class  Person(name:Int,Age:Int)
    def f1=(x:Int,y:Int)=>x+y

    def superMethod(f:(Int,Int)=>Int,value:Int):Int={
      return f(value,value)
    }
    var s=superMethod(f1,2)
    print(s)
  }
}
