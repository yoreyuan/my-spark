package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

//import org.slf4j.Logger

/**
  * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示
  *
  * Created by yore on 2019/2/20 15:32
  */
object Movie_Users_Analyzer_RDD extends App {

  // 设置日志的输出级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  var masterUrl = "local[2]"
  var dataPath = "demo/resources-data/ml-1m/"
//  dataPath = "/Users/yoreyuan/soft/my_project/spark-demo-2.x/demo/resources-data/ml-1m/"

  if(args.length >0 ){
    masterUrl = args(0)
  }else if(args.length >1){
    dataPath = args(1)
  }

  val conf = new SparkConf().setMaster(masterUrl)
    .setAppName("Movie_Users_Analyzer")


  /*val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext*/

  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().config(conf).getOrCreate()

  /**
    * 使用RDD，读取数据
    */
  // 用户ID::性别::年龄::职业::邮编代码
  val usersRDD = sc.textFile(dataPath + "users.dat")
  // 电影ID::电影名::电影类型
  val movieRDD = sc.textFile(dataPath + "movies.dat")
  val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
  // 用户ID::电影ID::评分数据::时间戳
  val ratingsRDD = sc.textFile(dataPath + "ratings.dat")



  /**
    * 电影点评系统用户分析之一，
    * 分析具体某部电影观看的用户信息，如电影ID为1193的用户信息（用户ID、Age、Gender、Occupation）
    *
    */
  val usersBasic : RDD[(String, (String, String, String))] = usersRDD
    .map(_.split("::"))
    .map(user => (user(3), (user(0), user(1), user(2))))

  for(elem <- usersBasic.collect().take(2)){
    println("usersBasicRDD (职业ID，(用户ID, 性别， 年龄))：" + elem)
  }

  val occupations: RDD[(String, String)] = occupationsRDD
    .map(_.split("::"))
    .map(job => (job(0), job(1)))

  for(elem <- occupations.collect().take(2)){
    println("occupationsRDD(职业ID，职业名)：" + elem)
  }

  val userInfomation: RDD[(String, ((String, String, String), String))] = usersBasic.join(occupations)
  userInfomation.cache()

  for(elem <- userInfomation.collect.take(2) ){
    println("userInfomation(职业ID，((用户ID， 性别， 年龄)， 职业名))：" + elem)
  }

  val targetMovie: RDD[(String, String)] = ratingsRDD
    .map(_.split("::"))
    .map(m => (m(0), m(1)))
    .filter(_._2.equals("1193"))

  for(elem <- targetMovie.collect.take(2)){
    println("targetMovie(用户ID，电影ID)：" + elem)
  }

  val targetUsers: RDD[(String, ((String, String, String), String))] = userInfomation
    .map(x => (x._2._1._1, x._2))
  for(elem <- targetUsers.collect.take(2)){
    println("targetUsers (用户ID，((用户ID, 性别， 年龄), 职业名))：" + elem)
  }

  println("\n电影点评系统用户行为分析，统计观看电影ID为1193的电影用户信息：用户ID、性别、年龄、职业名")

  val userInformationForSpecificMovie: RDD[(String, (String, ((String, String, String),String)))] = targetMovie
    .join(targetUsers)
  for(elem <- userInformationForSpecificMovie.collect.take(10)){
    println("userInformationForSpecificMovie(用户ID, (电影ID, ((用户ID, 性别， 年龄)， 职业名)))： " + elem)
  }
  println("=" * 20 )


  println("所有有电影中平均得分最高(口碑最好)的Top10电影：")
  val ratings = ratingsRDD.map(_.split("::"))
    // 用户ID::电影ID::评分数据::时间戳
    .map(r => (r(0), r(1), r(2)))
    .cache()  // 因为后面可能会多次使用，这里对ratings数据进行cache()

  //电影ID::电影名::电影类型
  val movies = movieRDD.map(_.split("::"))
      .map(m => (m(0), m(1), m(2))).cache()

  // (电影ID, (评分， 计数1))
  ratings.map(r => (r._2, (r._3.toDouble, 1) ))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(r => (r._2._1.toDouble/r._2._2, r._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)
  println("=" * 20 )


  println("所有电影中电影粉丝或者观看人数最多的电影：")
  ratings.map(r => (r._2, 1))
    .reduceByKey(_+_) // 电音ID, 电影人数
    .join(movies.map[(String, String)](m => (m._1, m._2)))  // (电影ID, (电影人数，电影名))
    .map(x => (x._2._1, x._2._2)) // 电影人数，电影名
    .sortByKey(false)
//    .map(x => (x._2, x._1)) // (电影名名, 电影人数）
    .take(10)
    .foreach(println)


  println("=" * 20 )
  println("分析最受男性喜爱的电影TOP10和最受女性喜爱的电影Top10：")
  val male = "M"
  val female = "F"
  // (用户ID, ((用户ID， 电影ID， 评分), 性别))
  val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3)))
    .join(
      usersRDD.map(_.split("::"))
        .map[(String, String)](x => (x(0), x(1))))
    .cache()
  genderRatings.take(2)foreach(println)

  val maleFilteredRatings : RDD[(String, String, String)] = genderRatings
    .filter(x => x._2._2.equals(male))
    .map(x => x._2._1)
  val femaleFilteredRatings : RDD[(String, String, String)] = genderRatings
    .filter(x => x._2._2.equals(female))
    .map(x => x._2._1)

  println("所有电影中最受男性喜欢的电影TOP10：")
  maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .map(x => (x._2._1.toDouble / x._2._2, x._1))
    .sortByKey(false)
    .take(10)
    .foreach(println)

  println("所有电影中最受女性喜欢的电影TOP10：")
  femaleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .map(x => (x._2._1.toDouble / x._2._2, x._1))
    .sortByKey(false)
    .take(10)
    .foreach(println)


  println("=" * 20 )
  println("通过RDD分析电影点评系统仿QQ和微信等用户群分析：")

  val targetQQUser = usersRDD.map(_.split("::"))
    .map(x => (x(0), x(2)))   //（用户ID，年龄）
    .filter(_._2.equals("18"))
  val targetTaobaoUser = usersRDD.map(_.split("::"))
    .map(x => (x(0), x(2)))
    .filter(_._2.equals("25"))
  /**
    * 在Spark中如何实现mapjoin呢？显然是要借助于Broadcast，吧数据广播到Executor级别，让改Executor上的所有任务共享唯一的数据，
    * 而不是每次运行Task的时候都发送一份数据的复制，这样显著降低了网络数据的传输和JVM内存的消耗。
    */
  val targetQQUuserSet = HashSet() ++ targetQQUser.map(_._1).collect()
  val targetTaobaoUsersSet = HashSet() ++ targetTaobaoUser.map(_._1).collect()
  val targetQQUsersBroadcast = sc.broadcast(targetQQUuserSet)
  val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)

  val movieTD2Name = movieRDD.map(_.split("::"))
    .map(x => (x(0), x(1)))
    .collect
    .toMap
  println("所有电影中QQ或微信核心目标用户最喜爱电影TopN分析：")
  ratings.filter(x => targetQQUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .map(x => (movieTD2Name.getOrElse(x._1, null), x._2))
      .foreach(println)


  println("所有电影中淘宝核心目标用户最喜爱电影TopN分析：")
  ratings.filter(x => targetTaobaoUsersBroadcast.value.contains(x._1))
    .map(x => (x._2, 1))
    .reduceByKey(_ + _)
    .map(x => (x._2, x._1))
    .sortByKey(false)
    .map(x => (x._2, x._1))
    .take(10)
    .map(x => (movieTD2Name.getOrElse(x._1, null), x._2))
    .foreach(println)


  println("=" * 20 )
  println("对电影评分数据以Timestamp和Rating两个维度进行二次排序：")
  val pairWithSortKey = ratingsRDD.map(line => {
    val splited = line.split("::")
    (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
  })
  val sorted = pairWithSortKey.sortByKey(false)
  val sortedResult = sorted.map(sortedline => sortedline._2)

  sortedResult.take(10).foreach(println)


}
