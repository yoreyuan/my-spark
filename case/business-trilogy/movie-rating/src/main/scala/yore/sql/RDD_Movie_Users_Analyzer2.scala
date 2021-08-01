package yore.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yore on 2019/1/3 14:34
  */
object RDD_Movie_Users_Analyzer2 {

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

    val usersRDD = sc.textFile("case/business-trilogy/movie-rating/src/main/resources/user.data")
    val moviesRDD = sc.textFile("case/business-trilogy/movie-rating/src/main/resources/movies.data")
    val ratingsRDD = sc.textFile("case/business-trilogy/movie-rating/src/main/resources/ratings.data")

    //TODO 电影数据的分析


    // (2.1). 分析最受男性喜爱的电影TOP10
    // (2.2). 分析最受女性喜爱的电影TOP10

    /**
      * 但从ratings中无法分析计算出最受男性或女性喜爱的电影TOP10，因为RDD中没有性别信息，
      * 如果需要使用性别信息进行Gender分类，此时一定需要聚合。
      * 当然，我们力求聚合使用的是mapjoin（分布式计算的一大痛点是数据倾斜，map端的join一定是不会发生数据倾斜的）
      *
      * 那么我们这里真的可以使用mapjoin吗？是不可以的
      * 因为map端的join是使用broadcast吧相对小的多的变量广播出去，这样可以减少一次shuffle，这里用户的数据非常多，所以只能使用正常的join
      *
      */
    val movieInfo = moviesRDD.map(_.split("\\|"))
      .map(x => (x(0), x(1))).cache()

    // 获取用户的id和性别
    val usersGender = usersRDD.map(_.split("\\|"))
      .map(x => (x(0), x(2)))
    // 获取MovieID和评分。用户id,电影id，评分
    val ratings = ratingsRDD.map(_.split("\t"))
      .map(x => (x(0), x(1), x(2))).cache()
    // 用户ID， （(用户ID，电影ID，评分), 性别)
     val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3)) )
      .join(usersGender).cache()

    // (273,((273,328,3),F))
//    genderRatings.take(10).foreach(println )

    /*
    * 分别过滤出男性和女性的记录进行处理
    *
    * 获取的结果是给定性别的数据的元组，(用户ID，电影ID，评分)
    */
    val maleFilteredRatings = genderRatings.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val femaleFilteredRatings = genderRatings.filter(x => x._2._2.equals("F")).map(x => x._2._1)
//    maleFilteredRatings.take(20).foreach(println)

    // 对RDD进行处理，
    println("所有电影中最受男性喜爱的电影Top10：")
    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .map(x => (x._1, x._2._1.toDouble / x._2._2))
        .join(movieInfo)
        .map(item => (item._2._1, item._2._2))
        .sortByKey(false)
        .take(10)
        .foreach(record => printf("%s评分为：%.1f\n", record._2, record._1))

    println("\n" + "-"*20 + "\n")
    println("所有电影中最受女性喜爱的电影Top10：")
    femaleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .join(movieInfo)
      .map(item => (item._2._1, item._2._2))
      .sortByKey(false)
      .take(10)
      .foreach(record => printf("%s评分为：%.1f\n", record._2, record._1))


    println("\n" + "="*26 + "\n")
    println("对电影评分数据以Timestamp和Rating两个维度进行二次降序排序")
    val pairWithSortKey = ratingsRDD.map(line => {
      val splited = line.split("\t")
      // 对 Timestamp和Rating两个维度进行二次降序排序
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble ), line)
    })
    // 直接调用sortByKey， 此时会按照之前实现的compare方法排序
    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.take(10).foreach(println)


    // 最后关闭SparkSession
    spark.stop()

  }

}
