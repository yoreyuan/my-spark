package yore.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by yore on 2019/3/13 17:33
  */
object RDD_demo {

  def main(args: Array[String]): Unit = {

    // 设置项目日志级别
    Logger.getLogger("org")
      .setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("RDD_demo")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

//    val sc = new SparkContext(conf)
    val sc = spark.sparkContext


    val data = Array(1, 2, 3, 4, 5)
    val line = sc.textFile("case/business-trilogy/movie-rating/src/main/resources/ad_clicked.txt")

//    val distData = sc.parallelize(data)
    val totalLengths = line.map(s => s.length)
    totalLengths.persist()
    val totalLength = totalLengths
      .reduce((a, b) => a+b)












  }

}
