package yore.DAG

import org.apache.spark.{SparkConf, SparkContext}

/**
  * DAG逻辑视图解析：
  *
  * 在程序运行前，Spark的DAG调度器会将整个流程设定为一个Stage,
  * 此Stage包含3个操作，
  * 5个RDD:
  *   MapPartitionRDD(读取文件数据时)
  *   MapPartitionRDD(flatMap操作)
  *   MapPartitionRDD(map操作)
  *   MapPartitionRDD(reduceByKey的local段操作)
  *   ShuffleRDD(reduceByKeyshuffle操作)
  *
  * 回溯的流程
  *   (1)在ShuffleRDD与MapPartitionRDD中存在shuffle操作，整个RDD先在此切开，形成两个Stage
  *   (2)继续向前回溯，MapPartitionRDD(reduceByKey的local段操作)与MapPartitionRDD(map操作)中间不存在Shuffle，归为同一个Stage
  *   (3)继续回溯，发现往前的所有RDD之间都不存在Shuffle，应归为同一个Stage
  *   (4)回溯完毕，形成DAG，由两个Stage构成
  *
  *
  *
  * Created by yore on 2019/1/23 13:10
  */
object DAGSchedulerDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("Wow,My first Spark App")
    conf.setMaster("local[1]")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("demo/business-practice/movie-rating/ReadMe.md", 1)

    val words = lines.flatMap(lines => lines.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.collect.foreach(println)

//    sc.stop()

  }

}
