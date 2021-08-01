package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * 12.11 纯粹通过DataSet对电影点评系统进行流行度和不同年龄阶段兴趣分析等
  *
  * Created by yore on 2019/2/24 16:03
  */
object Movie_Users_Analyzer_DS extends App {

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


  println("通过DataSet实现某特定电影观看者中男性和女性不同年龄的人数")

  // （1）定义用户、评分、电影case class类
  case class User(UserID: String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
  case class Rating(UserID: String, MovieID: String, Rating: Double, Timestamp: String)
  case class Movie(MovieID: String, Title: String, Genres: String)

  // (2) 将usersDataFrame、ratingsDataFrame分别转换为userDataSet、ratingsDataSet


  /** 使用StructType方式把用户数据格式化 */
  import spark.implicits._
  val schemaforusers = StructType(
    "UserID::Gender::Age::OccupationID::Zip_Code".split("::")
      .map(column => StructField(column, StringType, true))
  )
  val usersRDDRows : RDD[Row] = usersRDD.map(_.split("::"))
    .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
  val usersDataFrame : DataFrame = spark.createDataFrame(usersRDDRows, schemaforusers)
  val usersDataSet = usersDataFrame.as[User]

  /** 使用StructType方式把评分数据格式化 */
  val schemaforratings = StructType(
    "UserID::MovieID".split("::")
      .map(column => StructField(column, StringType, true))
  ).add("Rating", DoubleType, true)
    .add("Timestamp", StringType, true)
  val ratingsRDDRows : RDD[Row] = ratingsRDD.map(_.split("::"))
    .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
  val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)
  val ratingsDataSet = ratingsDataFrame.as[Rating]

  /** 使用StructType方式把电影数据格式化 */
  val schemaformovies = StructType(
    "MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true))
  )
  val moviesRDDRows : RDD[Row] = moviesRDD.map(_.split("::"))
    .map(line => Row(line(0).trim, line(1).trim, line(2).trim))
  val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)
  val moviesDataSet = moviesDataFrame.as[Movie]

  println("功能一： 通过DataSet实现某特定电影观看者中男性和女性不同年龄分别有多少人")
  ratingsDataSet.filter(s"MovieID=1193")
    .join(usersDataSet, "UserID")
    .select("Gender", "Age")
    .groupBy("Gender", "Age")
    .count()
    .show(10)


  println("=" * 20 )
  println("通过DataSet方式计算所有电影中评分得分最高（口碑最好）的电影TopN")
  ratingsDataSet.select("MovieID", "Rating")
    .groupBy("MovieID")
    .avg("Rating")
    .orderBy($"avg(Rating)".desc)
    .show(10)


  println("=" * 20 )
  println("通过DataSet方式计算所有电影中粉丝或者观人数最多（最流行电影）的电影TopN")
  ratingsDataSet.groupBy("MovieID")
    .count()
    .orderBy($"count".desc)
    .show(10)


  println("=" * 20 )
  println("通过DataSet方式实现所有电影中最受男性、女性喜爱的电影TopN")
  val genderRatingsDataSet = ratingsDataSet.join(usersDataSet, "UserID").cache()
  val maleFilteredRatingsDataSet = genderRatingsDataSet.filter("Gender='M'")
    .select("MovieID", "Rating")
  val femaleFilteredRatingsDataSet = genderRatingsDataSet.filter("Gender='F'")
    .select("MovieID", "Rating")

  maleFilteredRatingsDataSet.groupBy("MovieID")
    .avg("Rating")
    .orderBy($"avg(Rating)".desc)
    .show(10)

  femaleFilteredRatingsDataSet.groupBy("MovieID")
    .avg("Rating")
    .orderBy($"avg(Rating)".desc, $"MovieID".desc)
    .show(10)


  println("=" * 20 )
  println("通过DataSet方式实现所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析")
  ratingsDataSet.join(usersDataSet, "UserID")
    .filter("Age = '18'")
    .groupBy("MovieID")
    .count()
    .join(moviesDataFrame, "MovieID")
    .select("Title", "count")
    .sort($"count".desc)
    .show(10)

  println("通过DataSet方式实现所有电影中淘宝核心目标用户最喜爱电影TopN分析")
  ratingsDataSet.join(usersDataSet, "UserID")
    .filter("Age = '25'")
    .groupBy("MovieID")
    .count()
    .join(moviesDataFrame, "MovieID")
    .select("Title", "count")
    .sort($"count".desc)
    .limit(10)
    .show()





}
