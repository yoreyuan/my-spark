package yore.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从数据流动角度
  * RDD生成的内部机制解析
  *
  * Created by yore on 2019/1/25 17:10
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ALL)

    val conf = new SparkConf
    conf.setAppName("Wow, Word Count!")
    conf.setMaster("local[1]")

    val sc = new SparkContext(conf)

    // 读取数据并设置并行度为1
    val lines = sc.textFile("case/business-trilogy/movie-rating/src/main/resources/helloSpark.txt", 1)

    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCountsOdered = pairs.reduceByKey(_+_)
      .map( pairs => (pairs._2, pairs._1))
      .sortByKey(false)
      .map(pairs => (pairs._2, pairs._1))

    wordCountsOdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))

    sc.stop()


  }

}
