package yore.dt


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._

import scala.collection.Map

/**
  *
  * Created by yore on 2019/2/27 17:48
  */
object NBABasketball_Analysis {

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[2]"
    var dataPath = "demo/business-practice/NBA-player-data-analys/src/main/resources/"
    val data_path = dataPath + "data"
    val data_tmp =  dataPath + "tmp"

    if(args.length >0 ){
      masterUrl = args(0)
    }else if(args.length >1){
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl)
      .setAppName("NBA-player-data-analys")
    val spark = SparkSession.builder().config(conf)
      .appName("NBA-player-data-analys")
//      .config("spark.sql.warehouse.dir", "spark-warehouse")
//      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
//    import org.apache.spark.sql.functions._

    // Delete if the tmp directory exists
    FileSystem.get(new Configuration()).delete(new Path(data_tmp), true)

    for(year <- 2012 to 2018){
      val statsPerYear = sc.textFile(s"${data_path}/NBA_${year}_totals.csv")

      statsPerYear.filter(_.contains(","))
        .map(line => (year, line))
        .saveAsTextFile(s"${data_tmp}/${year}/")
    }

    // 读取所有NBA球员过去的历史数据
    val NBAStats = sc.textFile(s"${data_tmp}/*/*")

    // 第一步：对原始数据进行初步的清洗 && 第二步：对数据进行基础性处理
    val filteredStats : RDD[String] = NBAStats.filter(line => !line.contains("FG%"))
      .filter(line => line.contains(","))
      .map(line => line.replace(",,", ",0,")) // 清洗类似于"0,0,,93,"的数据
      .map(line => line.replace("*", "")) // 清洗类似于"Allen*"的数据
      .map(line => line.replace(",.", ",0.")) // 清洗类似于"0,2,.000,0,2"的数据

    filteredStats.collect.take(10).foreach(println)
//    filteredStats.persist(StorageLevel.MEMORY_AND_DISK)
    filteredStats.cache()

    /**
      * 统计出每年NBA球员比赛的各项聚合统计数据
      */
    val txtStart : Array[String] = Array(
      "FG", "FGA", "FG%",  // 投篮命中次数, 投篮出手次数, 投篮命中率
      "3P", "3PA", "3P%",   // 3分命中, 3分出手, 3分命中率
      "2P", "2PA", "2P%",   // 2分命中, 2分出手, 2分命中率
      "eFG%", "FT", "FTA", "FT%", "ORB",  // 有效投篮命中率, 罚球命中, 罚球出手次数, 罚球命中率, 进攻篮板球
      "DRB", "TRB", "AST", "STL", "BLK",  // 防御篮板球, 篮板球, 助攻, 抢断, 盖帽
      "TOV", "PF", "PTS" // 失误, 个人犯规, 得分
    )
    println("NBA球员数据统计维度：")
    txtStart.foreach(println)
    // 基础数据项，需要在集群中使用，因此会在后面广播出去
    val aggStats : Map[String, Double] = processStats(filteredStats, txtStart).collectAsMap()
    println("NBA球员基础数据项aggStats MAP映射集：")

    /**
      * (2018_PF_stdev, 52.77921146501997)
      * (2012_FG%_max, 1.0)
      * (2013_2P_max, 706.0)
      * (2014_STL_max, 163.0)
      * (2014_FG_avg, 160.77880184331815)
      * (2015_PTS_stdev, 443.60096103812543)
      * (2014_3P%_min, 0.0)
      * (2015_2PA_max, 1238.0)
      * (2015_TRB_count, 578.0)
      * (2017_3PA_stdev, 141.37791737920563)
      * (2016_FGA_max, 1941.0)
      * (2018_TOV_max, 304.0)
      * (2017_FTA_avg, 87.19126506024094)
      * (2012_ORB_max, 310.0)
      * (2012_2P%_min, 0.0)
      * (2013_BLK_stdev, 28.635408829704502)
      * (2016_2PA_min, 0.0)
      * (2012_3PA_count, 573.0)
      * (2015_3P%_count, 578.0)
      * (2012_2PA_avg, 283.870855148342)
      */
    aggStats.take(20).foreach{
      case (k, v) => println(" (" + k + ", "  + v +")")
    }
    // 将RDD转换成map结构进行广播
    val broadcastStats = sc.broadcast(aggStats)


    /**
      * NBA球员数据每年标准分Z-Score计算
      */
    // 解析统计，跟踪权重
    val txtStatZ = Array( "FG", "TF", "3P", "TRB", "AST", "STL", "BLK", "TOV", "PTS%")
    val zStats : Map[String, Double] = processStats(filteredStats, txtStatZ, broadcastStats.value).collectAsMap()
    println("\nNBA球员Z-Score标准分zStats MAP映射集：")

    /**
      * (2015_TRB_count, 578.0)
      * (2015_PTS%_avg, -8.428587688902311E-16)
      * (2018_TOV_max, 0.9605873714226074)
      * (2013_BLK_stdev, 1.0000000000000002)
      * (2017_TF_min, -59.55420180722903)
      * (2014_FG_avg, 7.479979964653161)
      * (2014_STL_max, 4.089815633960388)
      * (2012_TF_stdev, 19.1040519962441)
      * (2018_AST_avg, 1.0412677664550785E-15)
      * (2012_FG_stdev, 27.126385282680246)
      * (2018_BLK_max, 6.354678873886561)
      * (2013_TOV_avg, -2.42861286636753E-16)
      * (2012_FG_count, 573.0)
      * (2012_AST_max, 4.730276578594265)
      * (2018_FG_max, 139.84492977099265)
      * (2016_PTS%_count, 595.0)
      * (2013_AST_stdev, 0.9999999999999999)
      * (2015_TRB_stdev, 1.0000000000000004)
      * (2012_TF_count, 573.0)
      * (2012_TRB_min, -1.0037328834614843)
      */
    zStats.take(20).foreach{
      case (k, v) => println(" (" + k + ", "  + v +")")
    }
    // 将RDD转换为map结构，并使用广播到Executor
    val zBroadcastStats = sc.broadcast(zStats)


    /**
      * NBA球员数据每年归一化计算
      */
    // 解析统计，进行归一化处理
    val nStats : RDD[BballData] = filteredStats.map(x => bbParse(x, broadcastStats.value, zBroadcastStats.value))
    // 转换RDD为RDD[Row]，可以将其再转换为dataframe
    val nPlayer : RDD[Row] = nStats.map(x =>{
      val nPlayerRow : Row = Row.fromSeq(Array(x.name, x.year, x.age, x.position, x.team, x.gp, x.team, x.gs, x.mp)
        ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN)
      )
      println(nPlayerRow.mkString("\t"))
      nPlayerRow
    })
    //    nPlayer.foreach(r => println(r.mkString("\t")))

    // 为DataFrame创建模式Schema
    val schemaN : StructType = StructType(
      StructField("name", StringType, true):: StructField("year", IntegerType, true)::
      StructField("age", IntegerType, true) ::  StructField("position", StringType, true)::
        StructField("term", StringType, true) ::  StructField("gp", IntegerType, true)::
        StructField("gs", IntegerType, true) ::  StructField("mp", DoubleType, true)::
        StructField("FG", DoubleType, true) ::  StructField("FGA", DoubleType, true)::
        StructField("FGP", DoubleType, true) ::  StructField("3P", DoubleType, true)::
        StructField("3PA", DoubleType, true) ::  StructField("3PP", DoubleType, true)::
        StructField("2P", DoubleType, true) ::  StructField("3PA", DoubleType, true)::
        StructField("2PP", DoubleType, true) ::  StructField("eFG", DoubleType, true)::
        StructField("FT", DoubleType, true) ::  StructField("FTA", DoubleType, true)::
        StructField("FTP", DoubleType, true) ::  StructField("ORB", DoubleType, true)::
        StructField("DRB", DoubleType, true) ::  StructField("TRB", DoubleType, true)::
        StructField("AST", DoubleType, true) ::  StructField("STL", DoubleType, true)::
        StructField("BLK", DoubleType, true) ::  StructField("TOV", DoubleType, true)::
        StructField("PF", DoubleType, true) ::  StructField("PTS", DoubleType, true)::
        StructField("zFG", DoubleType, true) ::  StructField("zFT", DoubleType, true)::
        StructField("z3p", DoubleType, true) ::  StructField("zTRB", DoubleType, true)::
        StructField("zAST", DoubleType, true) ::  StructField("zSTL", DoubleType, true)::
        StructField("zBLK", DoubleType, true) ::  StructField("zTOV", DoubleType, true)::
        StructField("zPTS", DoubleType, true) ::  StructField("zTOT", DoubleType, true)::
        StructField("nFG", DoubleType, true) ::  StructField("nFT", DoubleType, true)::
        StructField("n3P", DoubleType, true) ::  StructField("nTRB", DoubleType, true)::
        StructField("nSTL", DoubleType, true) ::  StructField("nBLK", DoubleType, true)::
        StructField("nTOV", DoubleType, true) ::  StructField("nPTS", DoubleType, true)::
        StructField("nTOT", DoubleType, true) :: Nil
    )
    val dfPlayersT :  DataFrame = spark.createDataFrame(nPlayer, schemaN)
    // 将所有数据保存为临时表
    dfPlayersT.createOrReplaceTempView("tPlayers")
    // 计算exp和zdiff、ND2FF
    val dfPlayers : DataFrame = spark.sql(
      "select age-min_age as exp, tPlayers.* from tPlayers join" +
      "(select name, min(age) as min_age from tPlayers group by name) as tl" +
      " on tPlayers.name = tl.name order by tPlayers.name, exp"
    )
    println("计算exp and zdiff ， ndiff")
    dfPlayers.show()
    // 保存为表
    dfPlayers.createOrReplaceTempView("Players")

  }


  /**
    * 处理原始数据为 zScores and nScores
    *
    * @param stats0
    * @param txtStat
    * @param bStats
    * @param zStats
    * @return RDD[(String, Double)]
    */
  def processStats (stats0: org.apache.spark.rdd.RDD[String], txtStat: Array[String],
                    bStats: scala.collection.Map[String, Double] = Map.empty,
                    zStats: scala.collection.Map[String, Double] = Map.empty): RDD[(String, Double)] = {
    //解析stats
    val stats1 = stats0.map(x => bbParse(x, bStats, zStats))

    // 按照年份进行分组
    val stats2 = {
      if(bStats.isEmpty){
        stats1.keyBy(x => x.year)
          .map(x => (x._1, x._2.stats))
          .groupByKey()
      }else{
        stats1.keyBy(x => x.year)
          .map(x => (x._1, x._2.statsZ))
          .groupByKey()
      }
    }

    // 转换成StatCounter
    val stats3 = stats2.map{
      case (x, y) => (x, y.map(a => a.map(
        b => BballStatCounter(b)
      )))
    }

    // 合并
    val stats4 = stats3.map{
      case(x, y) => (x, y.reduce(
        (a, b) => a.zip(b).map{
          case(c, d) => c.merge(d)
        }
      ))
    }

    // combine 合并聚合
    val stats5 = stats4.map{
      case(x, y) => (x, txtStat.zip(y))
    }.map{
      x => (x._2.map({
        case(y, z) => (x._1, y, z)
      }))
    }

    // 使用逗号分隔符打印输出
    val stats6 = stats5.flatMap(x => x.map({
      y => (y._1, y._2, y._3.printStats(","))
    }))

    // 转换为key-value键值对
    val stats7 = stats6.flatMap{
      case(a, b, c) => {
        val pieces = c.split(",")
        val count = pieces(0)
        val mean = pieces(1)
        val stdev = pieces(2)
        val max = pieces(3)
        val min = pieces(4)
        Array(
          (a + "_" + b + "_" + "count", count.toDouble),
          (a + "_" + b + "_" + "avg", mean.toDouble),
          (a + "_" + b + "_" + "stdev", stdev.toDouble),
          (a + "_" + b + "_" + "max", max.toDouble),
          (a + "_" + b + "_" + "min", min.toDouble)
        )
      }
    }
    stats7
  }


  /**
    * 初始化 + 权重统计 + 归一统计
    */
  case class BballData(val year: Int, name: String, position: String, age: Int, team: String, gp: Int, gs: Int,
               mp: Double, stats: Array[Double], statsZ: Array[Double] = Array[Double](),
               valueZ: Double = 0, statsN: Array[Double] = Array[Double](), valueN: Double = 0, experience: Double =0)

  def bbParse(input: String, bStats: collection.Map[String, Double] = Map.empty,
              zStats: collection.Map[String, Double] = Map.empty) = {
    val line = input.replace(",,", ",0,")
    // (2013,234,Jonas Jerebko\jerebjo01,PF,26,DET,64,0,741,98,208,0.471,31,74,0.419,67,134,0.500,0.546,43,59,0.729,51,124,175,39,21,6,43,85,270)
    val pieces = line.substring(1, line.length - 1).split(",")
    val year = pieces(0).toInt
    val name = pieces(2)
    val position = pieces(3)  // 打球位置
    val age = pieces(4).toInt
    val team = pieces(5)
    val gp = pieces(6).toInt  // 上场次数
    val gs = pieces(7).toInt  // 首发次数
    val mp = pieces(8).toDouble  // 比赛时间（分钟）
    val stats = pieces.slice(9, 31).map(x => x.toDouble)
    var statsZ : Array[Double] = Array.empty
    var valueZ : Double = Double.NaN
    var statsN : Array[Double] = Array.empty
    var valueN : Double = Double.NaN

    if(!bStats.isEmpty){
      // 投篮命中次数
      val fg = (stats(2) - bStats.apply(year.toString + "_FG%_avg")) * stats(1)
      val tp = (stats(3) -bStats.apply(year.toString + "_3P_avg")) / bStats.apply(year.toString + "_3P_stdev")
      // 罚球命中
      val ft = (stats(12) - bStats.apply(year.toString + "_FT%_avg")) * stats(11)
      val trb =(stats(15) - bStats.apply(year.toString + "_TRB_avg")) / bStats.apply(year.toString + "_TRB_stdev")
      val ast =(stats(16) - bStats.apply(year.toString + "_AST_avg")) / bStats.apply(year.toString + "_AST_stdev")
      val stl =(stats(17) - bStats.apply(year.toString + "_STL_avg")) / bStats.apply(year.toString + "_STL_stdev")
      val blk =(stats(18) - bStats.apply(year.toString + "_BLK_avg")) / bStats.apply(year.toString + "_BLK_stdev")
      val tov =(stats(19) - bStats.apply(year.toString + "_TOV_avg")) / bStats.apply(year.toString + "_TOV_stdev") * (-1)
      val pts =(stats(21) - bStats.apply(year.toString + "_PTS_avg")) / bStats.apply(year.toString + "_PTS_stdev")

      statsZ = Array(fg, ft, tp, trb, ast, stl, blk, tov, pts)
      valueZ = statsZ.reduce(_ + _)

      if(!zStats.isEmpty){
        val zfg = (fg - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev")
        val zft = (ft - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev")
        val fgN = statNormalize(
            zfg,
          (zStats.apply(year.toString + "_FG_max") - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"),
          (zStats.apply(year.toString + "_FG_min") - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"))
        val ftN = statNormalize(
          zft,
          (zStats.apply(year.toString + "_FT_max") - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"),
          (zStats.apply(year.toString + "_FT_min") - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"))
        val tpN = statNormalize(tp, zStats.apply(year.toString + "_3P_max"), zStats.apply(year.toString + "_3P_min"))
        val trbN = statNormalize(trb, zStats.apply(year.toString + "_TRB_max"), zStats.apply(year.toString + "_TRB_min"))
        val astN = statNormalize(ast, zStats.apply(year.toString + "_AST_max"), zStats.apply(year.toString + "_AST_min"))
        val stlN = statNormalize(stl, zStats.apply(year.toString + "_STL_max"), zStats.apply(year.toString + "_STL_min"))
        val blkN = statNormalize(blk, zStats.apply(year.toString + "_BLK_max"), zStats.apply(year.toString + "_BLK_min"))
        val tovN = statNormalize(tov, zStats.apply(year.toString + "_TOV_max"), zStats.apply(year.toString + "_TOV_min"))
        val ptsN = statNormalize(pts, zStats.apply(year.toString + "_PTS_max"), zStats.apply(year.toString + "_PTS_min"))

        statsZ = Array(zfg, zft, tp, trb, ast, stl, blk, tov, pts)
        valueZ = statsZ.reduce(_ + _)
        statsN = Array(fgN, ftN, tpN, trbN, astN, stlN, blkN, tovN, ptsN)
        valueN = statsN.reduce(_ + _)

      }
    }

    BballData(year, name, position, age, team, gp, gs, mp, stats, statsZ, valueZ, statsN, valueN)

  }


  def statNormalize(stat: Double, max: Double, min: Double) = {
    val newmax = math.max(math.abs(max), math.abs(min))
    stat / newmax
  }


  /**
    * 该类是一个辅助工具类，后面编写业务代码的时候会反复使用其中的方法
    */
  class BballStatCounter extends Serializable{
    val stats : StatCounter = new StatCounter()
    var missing : Long = 0

    def add(x : Double) : BballStatCounter = {
      if(x.isNaN){
        missing += 1
      }else{
        stats.merge(x)
      }
      this
    }

    def merge(other: BballStatCounter): BballStatCounter = {
      stats.merge(other.stats)
      missing += other.missing
      this
    }

    def printStats(delim: String): String = {
      stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
    }

    override def toString: String = {
      "stats: " + stats.toString() + "NaN" + missing
    }
  }
  object BballStatCounter extends Serializable {
    /**
      * 这里使用了Scala语言的一个编程技巧，借助于apple工厂方法，在构造该对象的时候就可以执行结束
      */
    def apply(x: Double) = new BballStatCounter().add(x)
  }




}
