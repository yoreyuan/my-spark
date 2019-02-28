package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yore on 2019/2/26 13:01
  */
object EB_Users_Analyzer_DS extends App{

  // 设置日志的输出级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  var masterUrl = "local[2]"
  var dataPath = "demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/"

  if(args.length >0 ){
    masterUrl = args(0)
  }else if(args.length >1){
    dataPath = args(1)
  }

  val conf = new SparkConf().setMaster(masterUrl)
    .setAppName("E-commerce-interactive-analysis-system")
  val spark = SparkSession.builder().config(conf)
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._
  import org.apache.spark.sql.functions._


//  // {"userID":0,"name":"spark0","registeredTime":"2016-10-11 18:06:25"}
//  case class Users(userID: Long, name: String, registeredTime: String)
//  // {"logID":"01","userID":0,"time":"2016-10-17 15:42:45","type":1,"consumed":33.36}
//  case class Logs(logID: String, userID: Long, time: String, typed: Long, consumed: Double)
//  val persons = spark.read.json(dataPath + "log.json").as[Logs]
//  // overwrite，append，ignore
//  persons.toDF().write.format("parquet").mode("overwrite").save(dataPath + "data/") // 将数据保存为parquet格式
//  persons.printSchema()

  /** 方法一：通过读取json格式的数据文件加载数据 */
  /*val userInfo = spark.read.format("json").json(dataPath + "user.json")
  val userLog = spark.read.format("json").json(dataPath + "log.json")
  println("用户信息及用户访问记录文件JSON格式：")
  userInfo.printSchema()
  userLog.printSchema()*/


  /** 方法二：通过读取parquet格式的数据文件加载数据 */
  val userInfo = spark.read.format("parquet").parquet(dataPath + "user.parquet")
  val userLog = spark.read.format("json").json(dataPath + "log.json")
  println("用户信息及用户访问记录文件 parquet 格式：")
  userInfo.printSchema()
  userLog.printSchema()


  // 14.1 纯粹通过DataSet进行电商交互式分析系统中特定时段访问次数TopN
  println("1、统计特定时段访问次数最多的Top5：例如 2016-10-01 ~ 2016-11-01")
  val startTime = "2016-10-01"
  val endTime = "2016-11-30"
  userLog.filter(
    "time >= '" + startTime + "' and time <= '" + endTime + "' and typed = 0"
  ).join(userInfo, userInfo("userID") === userLog("userID"))
    .groupBy(userInfo("userID"), userInfo("name"))
    .agg(count(userLog("logID")).alias("userLogCount"))
    .sort($"userLogCount".desc)
    .limit(5)
    .show()


  // 14.2 纯粹通过DataSet分析特定时段购买金额Top10和访问次数增长Top10
  println("2、纯粹通过DataSet分析特定时段购买金额Top10和访问次数增长TopN")
  userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "' ")
    .join(userInfo, userInfo("userID") === userLog("userID"))
    .groupBy(userInfo("userID"), userInfo("name"))
    .agg(round(sum(userLog("consumed")), 2).alias("totalCount"))
    .sort($"totalCount".desc)
    .limit(5)
    .show()

  // 统计分析特定数段访问次数增长Top10的实现方法：
  case class UserLog(logID: String, userID: Long, time: String, typed: Long, consumed: Double)
  case class LogOnce(logID: String, userID: Long, count: Long)
  case class ConsumedOnce(logID: String, userID: Long, consumed: Double)
  println("统计特定时间段用户访问次数增长排名TopN:")
  val userAccessTemp = userLog.as[UserLog]
    .filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = 0 ")
    .map(log => LogOnce(log.logID, log.userID, 1))
    .union(
      userLog.as[UserLog]
        .filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = 0 ")
        .map(log => LogOnce(log.logID, log.userID, -1))
    )
  userAccessTemp.join(userInfo, userInfo("userID") === userAccessTemp("userID"))
    .groupBy(userInfo("userID"), userInfo("name"))
    .agg(round(sum(userAccessTemp("count")), 2).alias("viewIncreasedTmp"))
    .sort($"viewIncreasedTmp".desc)
    .limit(10)
    .show()




  // 14.3 纯粹通过DataSet进行电商交互式分析系统中各种类型TopN分析实战详解
  println("统计特定时段购买金额最多的Top5")
  userLog.filter("time >= '2016-10-01' and time <= '2016-11-01' and typed = 0 ")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort($"totalConsumed".desc)
      .limit(5)
      .show()

  println("特定时段访问次数增长最多的Top5用户")
  val userLogDS = userLog.as[UserLog]
      .filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = 0 ")
      .map(log => LogOnce(log.logID, log.userID, 1))
      .union(
        userLog.as[UserLog]
          .filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = 0 ")
          .map(log => LogOnce(log.logID, log.userID, -1))
      )
//  userLogDS.join(userLog, userLog("userID") === userLogDS("userID")).show(200)
  userLogDS.join(userInfo, userLogDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLogDS("count")).alias("viewCountIncreased"))
      .sort($"viewCountIncreased".desc)
      .limit(5)
      .sort()

  println("统计特定时间段购买金额增长最多的Top5用户")
  val userLogConsumerDS = userLog.as[UserLog]
      .filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = 0 ")
      .map(log => ConsumedOnce(log.logID, log.userID, log.consumed))
      .union(
        userLog.as[UserLog]
          .filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = 0 ")
          .map(log => ConsumedOnce(log.logID, log.userID, log.consumed))
      )
  userLogConsumerDS.join(userInfo, userLogConsumerDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLogConsumerDS("consumed")), 2).alias("viewConsumedIncreased"))
      .sort($"viewConsumedIncreased")
      .limit(5)
      .show()

  println("特定时间段注册之后，前两周内访问最多的Top10")
  userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(
        userInfo("registeredTime") >= "2016-10-11" &&
        userLog("time") >= userInfo("registeredTime") &&
        userLog("time") >= date_add(userInfo("registeredTime"), 14) &&
        userLog("typed") === 0
      )
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("logTimes"))
      .sort($"logTimes".desc)
      .limit(10)
      .show()

  println("特定时段注册之后，前两周内购买总额最多的Top10")
  userLog.join(userInfo, userInfo("userID") === userLog("userID"))
    .filter(
        userInfo("registeredTime") >= "2016-10-01" &&
        userInfo("registeredTime") <= "2016-10-08" &&
        userLog("time") >= userInfo("registeredTime") &&
        userLog("time") >= date_add(userInfo("registeredTime"), 14) &&
        userLog("typed") === 0
    ).groupBy(userInfo("userID"), userInfo("name"))
    .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
    .sort($"totalConsumed".desc)
    .limit(10)
    .show()


}
