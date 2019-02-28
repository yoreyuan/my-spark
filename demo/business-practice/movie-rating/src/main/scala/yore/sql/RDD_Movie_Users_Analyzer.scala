package yore.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yore on 2019/1/3 14:34
  */
object RDD_Movie_Users_Analyzer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("RDD_Movie_Users_Analyzer")

    /**
      * Spark 2.0之后引入SparkSession，封装了SparkContext和SQLContext，
      * 并且会在builder的getOrCreate方法中判断是否有符合要求的SparkSession存在，有则使用，没有则创建。
      *
      */
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    val sc = SparkContext(conf)
    val sc = spark.sparkContext
    // 设置spark程序运行的日志级别，
    sc.setLogLevel("WARN")

    val usersRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/user.data")
    val moviesRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/movies.data")
    val ratingsRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/ratings.data")

    //TODO 电影数据的分析

    // (1). 所有电影中评分最高的前10个电影名和平均评分
    println("所有电影中平均得分最高的（口碑最好的）的电影：")
    // 获取MovieID和Movie Name
    val movieInfo = moviesRDD.map(_.split("\\|"))
      .map(x => (x(0), x(1))).cache()
    // 获取MovieID和评分
    val ratings = ratingsRDD.map(_.split("\t"))
      .map(x => (x(0), x(1), x(2))).cache()

    // 计算电影评分和评论数 result格式为 （MovieID, (Sum(ratings), Count)）
    val moviesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // 计算每个电影的平均分
    val avgRatings = moviesAndRatings.map(x => (x._1, x._2._1.toDouble/x._2._2))

    avgRatings.join(movieInfo)
        .map(item => (item._2._1, item._2._2))
        .sortByKey(false)
        .take(12)
        .foreach(record => printf("%s评分为：%.1f\n", record._2, record._1))





    // 最后关闭SparkSession
    spark.stop()

  }

}
