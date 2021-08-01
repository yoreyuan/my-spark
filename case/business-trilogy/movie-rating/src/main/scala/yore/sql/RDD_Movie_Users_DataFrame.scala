package yore.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 公国DataFrame实战电影点评系统案例
  *
  * Created by yore on 2019/1/3 14:34
  */
object RDD_Movie_Users_DataFrame {

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

    println("功能一： 通过DataFrame实现某部电影观看者中男女性不同年龄人数 ")
    val schemaForUsers = StructType(
      "UserID|Age|Gender|OccupationID|Zip-code".split("\\|")
        .map(column => StructField(column, StringType, true))
    )
    // 然后我们把每一条数据变成以Row为单位的数据
    val usersRDDRows = usersRDD.map(_.split("\\|"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    //基于SparkSession的createDataFrame方法，结合Row和StructType的元数据信息
    // 基于RDD创建DataFrame，这是RDD就有了源数据信息的描述
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // 也可以对StructType调用add方法来对不同的StructField赋予不同的类型
    val schemaForRatings = StructType(
      "UserID\tMovieID".split("\\t")
        .map(column => StructField(column, StringType, true))
    ).add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)
    // 然后我们把每一条数据变成以Row为单位的数据
    val ratingsRDDRows = ratingsRDD.map(_.split("\\t"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim ))
    //基于SparkSession的createDataFrame方法，结合Row和StructType的元数据信息
    // 基于RDD创建DataFrame，这是RDD就有了源数据信息的描述
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaForRatings)

    // 构建Movie的DataFrame
    val movieForUsers = StructType(
      "MovieID\tTitle\tGenress".split("\\t")
        .map(column => StructField(column, StringType, true))
    )
    // 然后我们把每一条数据变成以Row为单位的数据
    val moviesRDDRows = moviesRDD.map(_.split("\\t"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim ))
    //基于SparkSession的createDataFrame方法，结合Row和StructType的元数据信息
    // 基于RDD创建DataFrame，这是RDD就有了源数据信息的描述
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, movieForUsers)


    //① 通过列名MovieID为xx的过滤出这部电影，这些列名就是上面指定的
    ratingsDataFrame.filter(s"MovieID = 1081")
      .join(usersDataFrame, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)


    /**
      * ② 通过SQL语句实现
      *
      * 注意：
      *     createTempView创建的临时表是会话级别的，当会话结束后这个临时表也就消失了，
      *
      *     如果想创建一个Application级别的临时表，需用createGlobalTemView，
      *     但是sql中标前面必须加 global_temp.表名
      *
      */
    println("功能二： 用LocalTempView实现莫不电影观看者中不同性别不同年龄分别有多少人 ")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    val sql_local = "SELECT Gender,Age,count(*) from users u join ratings as r on u.UserID = r.UserID where MovieID = 1081 group by Gender,Age"
    spark.sql(sql_local).show(10)



    //③ 引用一个隐式转换来实现更复杂的功能
    import spark.sqlContext.implicits._
    ratingsDataFrame.select("MovieID", "Rating")
        .groupBy("MovieID")
        .avg("Rating")
        .orderBy($"avg(Rating)".desc).show(10)



    // DataFrame中间某一步转RDD操作
    ratingsDataFrame.select("MovieID", "Rating")
        .groupBy("MovieID")
        .avg("Rating")
        .rdd
        .map(row => (row(1), (row(0), row(1))))
        .sortBy(_._1.toString.toDouble, false)
        .collect
        .take(10)
        .foreach(println)



    // 最后关闭SparkSession
    spark.stop()

  }

}
