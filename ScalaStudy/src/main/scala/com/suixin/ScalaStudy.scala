package com.suixin

object ScalaStudy {
  //主函数
  def main(args: Array[String]): Unit = {
    val a=222
    var  b:String="dsa dsa"
    var s=if (b.equals("dsadsa")) "dsda" else "66"
    print(s)
    //块表达式
    var block={
      "sdas"
    }
    print(block)

    for (a<-1 to 20)
      print("dsd")
  }

  //定义方法
  def  myMethod(a:Int,b:Int) : String={
    for (i<-Array(1,2,3,43,4,3,43,4,3)if i>40){
      print("22222222222!!!!")
    }
    return "sds"
  }

  //定义函数
  val f1=(a:String,b:String)=>{a+b}
  print(f1("dsa","11111111"))
  print(myMethod(1222,23))

    var list=Array(1,23,4,34,5)
    list.map(_*10).reduce((a,b)=>a+b)

  var a=Seq("dsd","dasd",22)
  var b= a.flatMap(a=>a.toString.split("d")).reduce((a,b)=>a+b)





}
