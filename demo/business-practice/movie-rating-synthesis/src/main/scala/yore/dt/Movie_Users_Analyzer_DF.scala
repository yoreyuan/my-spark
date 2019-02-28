package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  *
  * Created by yore on 2019/2/22 14:36
  */
object Movie_Users_Analyzer_DF {

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[2]"
    var dataPath = "demo/resources-data/ml-1m/"

    if(args.length >0 ){
      masterUrl = args(0)
    }else if(args.length >1){
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl)
      .setAppName("Movie_Users_Analyzer_DF")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    /**
      * 使用RDD，读取数据
      */
    // 用户ID::性别::年龄::职业::邮编代码
    val usersRDD = sc.textFile(dataPath + "users.dat")
    // 电影ID::电影名::电影类型
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    // 用户ID::电影ID::评分数据::时间戳
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /** 使用StructType方式把用户数据格式化 */
    val schemaforusers = StructType(
      "UserID::Gender::Age::OccupationID::Zip-code".split("::")
        .map(column => StructField(column, StringType, true))
    )
    val usersRDDRows : RDD[Row] = usersRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaforusers)

    /** 使用StructType方式把评分数据格式化 */
    val schemaforratings = StructType(
      "UserID::MovieID".split("::")
        .map(column => StructField(column, StringType, true))
    ).add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)
    val ratingsRDDRows : RDD[Row] = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

    /** 使用StructType方式把电影数据格式化 */
    val schemaformovies = StructType(
      "MovieID::Title::Genres".split("::")
        .map(column => StructField(column, StringType, true))
    )
    val moviesRDDRows : RDD[Row] = moviesRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim))
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)


    // 查询出电影ID等于1193的数据，可以看出同步电影中男性和女性不同年龄分别有多少人
    ratingsDataFrame.filter(s"MovieID = 1193")  //这里DF里已经有MovieID的元数据信息
      .join(usersDataFrame, "UserID")   // 直接指定基于UserID连接
      .select("Gender", "Age")    // 对Gender和Age数据进行筛选
      .groupBy("Gender", "Age")
      .count()
      .show(10)


    println("=" * 20 )
    println("功能二：用GlobalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人")
    ratingsDataFrame.createGlobalTempView("ratings")
    usersDataFrame.createGlobalTempView("users")
    spark.sql("SELECT Gender,Age,Count(*) from global_temp.users u join global_temp.ratings as r on u.UserID=r.UserID where MovieID = 1193 group by Gender,Age")
        .show(10)

    println("功能二：用LocalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    spark.sql("SELECT Gender,Age,Count(*) from users u join ratings as r on u.UserID=r.UserID where MovieID = 1193 group by Gender,Age")
      .show(10)


    println("=" * 20 )
    println("通过DataFrame和RDD结合的方法计算所有电影中平均得分最高的电影TopN")
    // 方式一：通过DataFrame和RDD结合的方法
    ratingsDataFrame.select("MovieID", "Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .rdd
      .map(row => (row(1), (row(0), row(1))))   //  (评分, (电影ID, 评分))
      .sortBy(_._1.toString.toDouble, false)
      .map(tuple => tuple._2)
      .collect
      .take(10)
      .foreach(println)

    println("纯粹使用DataFrame方式计算计算所有电影中平均得分最高的电影TopN")
    import spark.sqlContext.implicits._
    ratingsDataFrame.select("MovieID", "Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)/*.explain(true)*/
      .show(10)


    // 12.8 通过Spark SQL下的两种不同方式实现最流行电影分析
    println("=" * 20 )
    println("通过DataFrame和RDD结合的方法计算最流行电影（即所有电影中粉丝或者观看人数最多的电影TopN）")
    // 方式一：通过DataFrame和RDD结合的方法
    ratingsDataFrame.select("MovieID", "Timestamp")
      .groupBy("MovieID")
      .count()
      .rdd
      .map(row => (row(1).toString.toLong, (row(0), row(1))))
      .sortByKey(false)
      .map(tuple => tuple._2)
      .collect()
      .take(10)
      .foreach(println)

    println("纯粹通过DataFrame的方法计算最流行的电影（即多有电影中粉丝或者观看人数最多）的电影TopN：")
    ratingsDataFrame.groupBy("MovieID")
      .count()
      .orderBy($"count".desc)
      .show(10)


    // 12.9 通过DataFrame分析最受男性和女性喜爱电影TopN
    println("=" * 20 )
    println("通过DataFrame分析最受男性喜爱的电影TopN")
    val genderRatingsDataFrame = ratingsDataFrame.join(usersDataFrame, "UserID").cache()
    val maleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender='M'")
      .select("MovieID", "Rating")
    maleFilteredRatingsDataFrame.groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    println("通过DataFrame分析最女男性喜爱的电影TopN")
    val femaleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender='F'")
      .select("MovieID", "Rating")
    femaleFilteredRatingsDataFrame.groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc, $"MovieID".desc)
      .show(10)


    println("=" * 20 )
    println("12.10 通过DataFrame分析电影点评系统仿QQ和微信等用户群")
    genderRatingsDataFrame.filter("Age = '18'")
      .groupBy("MovieID")
      .count()
      .orderBy($"count".desc)
      .printSchema()
    genderRatingsDataFrame.filter("Age='18'")
      .groupBy("MovieID")
      .count()
      .join(moviesDataFrame, "MovieID")
      .select("Title", "count")
      .orderBy($"count".desc)
      .show(10)

    genderRatingsDataFrame.filter("Age='25'")
      .groupBy("MovieID")
      .count()
      .join(moviesDataFrame, "MovieID")
      .select("Title", "count")
      .orderBy($"count".desc)
      .show(10)

  }

}
