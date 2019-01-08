package com.suixin

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import java.util.Date
import scala.math._
import java.io._



object AlsTrain{
    //als param
    var viewT = 2.0;
    var favoriteT = 2.0;
    var orderT = 2.0;
    var viewWeight = 0.1
        var favoriteWeight = 0.3
        var orderWeight = 0.6
        var rank = 5;
    var iterations = 10;
    var lambda = 1.0;
    var implicitPrefs = false;
    var userBlocks = 10;
    var productBlocks = 10;
    var path = "/home/mart_cd/wcy/model"
        var pathcsv = "file:///home/mart_cd/wcy/model"
var rmdnum = 5
        var modelnum = 0


        def CovertList2Str(rl: List[Int]): String = {
            var tpMap: Map[Int, List[Int]] = Map()
                for (x <- rl) {
                    val d = x / 100;
                    val m = x % 100;
                    if (tpMap.contains(m)) {
                        var tpList = tpMap(m);
                        tpList = tpList :+ d;
                        tpMap -= (m);
                        tpMap += (m -> tpList)
                    } else {
                        var tpList: List[Int] = List();
                        tpList = tpList :+ d;
                        tpMap += (m -> tpList);
                    }
                }

            var resList: List[String] = List();
            tpMap.keys.foreach { tp =>
                var item = tp + ":["
                    for (x <- tpMap(tp)) {
                        item = item + x + ",";
                    }
                item = item.substring(0, item.length - 1)
                    item = item + "]"
                    resList = resList :+ item;
            }
            var result = "{";
            for (x <- resList) {
                result = result + x + ",";
            }
            result = result.substring(0, result.length - 1)
                result += "}"
                return result
        }

    def tanh(v:Double,t:Double): Double = {
        (1.0-pow(2.71828,-v/t))/(1.0+pow(2.71828,-v/t))
    }

    def ParParm(args: Array[String]): Unit = {
        if (args.length != 12) {
            println("parm error! ");
            println("viewT favoriteT orderT viewWeight favoriteWeight orderWeight  rank iterations lambda userBlocks productBlocks");
            return;
        }

        viewT = args(0).toDouble;
        favoriteT = args(1).toDouble;
        orderT = args(2).toDouble;
        viewWeight = args(3).toDouble;
        favoriteWeight = args(4).toDouble;
        orderWeight = args(5).toDouble;
        rank = args(6).toInt;
        iterations = args(7).toInt;
        lambda = args(8).toDouble;
        userBlocks = args(9).toInt;
        productBlocks = args(10).toInt;
rmdnum = args(11).toInt
        modelnum = args(12).toInt

            val viewTDisplay = "viewT:" + viewT;
        val favoriteTDisplay = "favoriteT:" + favoriteT
            val orderTDisplay = "orderT:" + orderT
            val viewWeightDisplay="viewWeight:" + viewWeight
            val favoriteWeightDisplay="favoriteWeight:" + favoriteWeight
            val orderWeightDisplay="orderWeight:" + orderWeight
            val rankDisplay = "rank:" + rank
            val numinterDisplay = "numinter:" + iterations
            val lamdaDisplay = "lamda:" + lambda
            val userblocknumDisplay = "user_block_num:" + userBlocks
            val productblocknumDisplay = "product_block_num:" + productBlocks
val rmdnumdisplay = "modelnum:"+rmdnum
            val modelnumdisplay = "modelnum:"+modelnum

            println(viewTDisplay);
        println(favoriteTDisplay);
        println(orderTDisplay);
        println(viewWeightDisplay);
        println(favoriteWeightDisplay);
        println(orderWeightDisplay);
        println(rankDisplay);
        println(numinterDisplay);
        println(lamdaDisplay);
        println(userblocknumDisplay);
        println(productblocknumDisplay);
println(rmdnumdisplay)
        println(modelnumdisplay)

            var modelpath = path+modelnum+"/"+"train_p arm.txt"
            val writer = new PrintWriter(new File(modelpath))
            writer.println(viewTDisplay)
            writer.println(favoriteTDisplay)
            writer.println(orderTDisplay)
            writer.println(viewWeightDisplay)
            writer.println(favoriteWeightDisplay)
            writer.println(orderWeightDisplay)
            writer.println(rankDisplay)
            writer.println(numinterDisplay)
            writer.println(lamdaDisplay)
            writer.println(userblocknumDisplay)
            writer.println(productblocknumDisplay)
writer.println(rmdnumdisplay)
            writer.println(modelnumdisplay)
            writer.close()
    }

    def main(args: Array[String]) {
        ParParm(args)
            run()
    }

    def run() {


        val spark:SparkSession = SparkSession
            .builder()
            .appName("als-train-predict")
            .config("spark.some.config.option", "some-value")
            .enableHiveSupport()
            .getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            println("################################import spark.sql")
            import spark.sql
            import spark.implicits._
            sql("USE app")

            sql("DROP TABLE IF EXISTS  app_jshop_template_als_predict")
            sql("CREATE TABLE IF NOT EXISTS app_jshop_template_als_predict (user INT,count INT,r_result STRING,result STRING)")


            val query = "SELECT vender_id,tp_id,tp_type,view_count,favorite_count,order_count FROM app_jshop_template_recommend_behavior WHERE vender_id is not NULL AND tp_id is not NULL AND tp_type is not NULL AND view_count is not NULL AND favorite_count is not NULL AND order_count is not NULL"

            val rdf = sql(query)
            rdf.limit(5).show()

            val ratDf = rdf.map{fields => Rating(fields(0).toString.toInt
                        ,fields(1).toString.toInt*100+fields(2).toString.toInt 
                        ,10.0*(viewWeight*tanh(fields(3).toString.toDouble,viewT)+favoriteWeight*tanh(fields(4).toString.toDouble,favoriteT)+orderWeight*tanh(fields(5).toString.toDouble,orderT))
                        ) }.cache()

            ratDf.show(100)
            val ratings = ratDf.rdd

            val scorepath=path+modelnum+"/"+"score.txt"
println(scorepath)
            //val scoreWriter = new PrintWriter(new File(scorepath))
            //val scoreRdd = ratings.map{case Rating(user, product, rate) =>(user+","+ product+","+rate.formatted("%.2f"))}
//println("################################save score")
       // for(x<-scoreRdd.collect())
       // {
//println("111111")
                //scoreWriter.println(x)
        //}
        //scoreWriter.close()



println("sss count")
            val numRatings = ratings.count()
println("sss numRatings")
            val numUsers = ratings.map(_.user).distinct().count()
println("sss numUsers")
            val numTemplate = ratings.map(_.product).distinct().count()
println("sss numTemplate")


println("################################create run.txt")
            var runpath = path+modelnum+"/"+"run.txt" 
            val writer = new PrintWriter(new File(runpath))
            writer.println("numUsers:"+numUsers)
            writer.println("numTemplate:"+numTemplate)

            println("################################train model")
           // val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
            val modelExplicit = new ALS()
            .setRank(rank)
            .setIterations(iterations)
            .setLambda(lambda)
            .setImplicitPrefs(implicitPrefs)
            .setUserBlocks(userBlocks)
            .setProductBlocks(productBlocks)
            .run(ratings)

        println("################################create predict input")
            val testUsersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)}
        println("################################predict....")
        val predictions = modelExplicit.predict(testUsersProducts).map { case Rating(user, product, rate)=>((user, product), rate)}
        println("################################rating join predict")
        val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)

           //println("################################save rates and  predict")
            //val ratesAndPredsPath=path+modelnum+"/"+"rate_predict.txt"
            //val ratesAndPredsWriter = new PrintWriter(new File(ratesAndPredsPath))
            //val ratesAndPredsRdd = ratesAndPreds.map { case ((user, product), (r1, r2)) => (user+","+product+","+r1.formatted("%.2f")+","+r2.formatted("%.2f"))}
        
//println("################################save rates and predict")
        //for(x<-ratesAndPredsRdd.collect())
        //{
                //ratesAndPredsWriter.println(x)
        //}
        //ratesAndPredsWriter.close()


println("################################caulate mse")
            val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>(r1 - r2)*(r1 - r2)}.mean()
            writer.println("mse:"+MSE)

            var resultpath = path+modelnum+"/"+"result.txt" 
            val resultwriter = new PrintWriter(new File(resultpath))

            val user_list = rdf.groupBy("vender_id").count()
            user_list.limit(5).show()
            val rows=user_list.collect()
            val cols=user_list.columns
            for(i <- 0 to rows.length-1)
            {     

                val user_id = rows(i)(0).toString().toInt;
                println("##########################################start recommend user_id:"+user_id+"all:"+rows.length+"/"+i);
                val count = rows(i)(1).toString().toInt;
                val result = modelExplicit.recommendProducts(user_id,rmdnum); 
                val rr =  new StringBuilder; 
                var resList: List[Int] = List[Int]();
                val dd = result.map{ case Rating(user, product, rate) => {rr++=(product.toString()+"#"+rate.formatted("%.2f")+",");
                    resList=resList :+ product.toString.toInt;
                    println(user.toString());println(rate);}}
                //val d = result.map{ case Rating(user, product, rate) => (product)}
                println("################################dd length:"+dd.length); 
                rr.deleteCharAt(rr.length()-1);
                val ss = rr.toString();             
                val s = CovertList2Str(resList);
                resultwriter.println(user_id+"-"+count+"-"+ss+"-"+s)
            }
        resultwriter.close()


            //val df = spark.read.json("file:///usr/local/spark/examples/src/main/resources/people.json")

            //df.rdd.repartition(1).saveAsTextFile("file:///home/hadoop/submit_layout_data7.csv")
            //val csv_data = spark.read.csv("file:///home/hadoop/submit_layout_data7.csv")
            //csv_data.show()

            writer.close()

    }
}
