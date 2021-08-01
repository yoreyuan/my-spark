package yore.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * nc -lk 19999
  *
  * 结构化流 @see <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html">Structured Streaming Programming</a>
  *
  * <pre>
  *   Structured Streaming 的流关联
  *
  *   自Spark 2.3开始，Spark Structured Streaming开始支持Stream-stream Joins。
  *   两个流之间的join与静态的数据集之间的join有一个很大的不同
  *
  *   Spark Structured Streaming支持任何列之间的join，但是这样会带来一个问题：随着stream的长期运行，stream的状态数据会无限制地增长，并且这些状态数据不能被释放，
  *   不管多么旧的数据，在未来某个时刻都有可能会被join到，因此我们必须在join上追加一些额外的条件来改善state无止境的维持旧有输入数据的问题。
  *
  *     1. 定义waterwark, 抛弃超过约定时限到达的输入数据。
  *     2. 在事件时间上添加约束，约定数据多旧之后就不再参与join了，这种约束可以通过以下两种方式之一来定义：
  *       ① 指定事件时间的区间：例如：JOIN ON leftTime BETWEN rightTime AND rightTime + INTERVAL 1 HOUR
  *       ② 指定事件时间的窗口：例如：JOIN ON leftTimeWindow = rightTimeWindow

  *
  * </pre>
  *
  *
  * Created by yore on 2019/4/8 11:00
  */
object ss_performance {

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Structure Streaming Performance")
      .master("local")
      .getOrCreate()

    import spark.implicits._



    // Create DataFrame representing the stream of input lines from connection to localhost:9999。输入表
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    /**
      * Split the lines into words
      *
      * root
      * |-- value: string (nullable = true)
      */
    val words:  Dataset[String] = lines.as[String]
      .flatMap(line =>{
        println(s"* \t " + line)
        line.split(" ")
      })


    /**
      * Structured Streaming + Kafka Integration
      * http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
      */
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.100.164:9092")
      .option("subscribe", "example")
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "100")
      .load()

    val words2 = df.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")  // 没有转换则是字节数组
      .as[(String, String)]
      .flatMap(_._2.split(" "))
      .toDF("name")

//    spark.sqlContext.udf.register("getKafkaValue", getKafkaValue)
//    spark.sql("set spark.sql.crossJoin.enabled=true")
//    val staticTable = spark.sql("select * from source")
//    staticTable.show()

    // Generate running word count。结果表
//    val wordCounts: DataFrame = words.groupBy("UserID").count()
//    val wordCounts: DataFrame = words.groupBy("title").count()
//      val wordCounts: DataFrame = words2.groupBy("name").count()
    val wordCounts: DataFrame = words.join(words2, words2("name") === words("value"), joinType = "inner")


//    val wordsWatermark = words.withWatermark("t1", "1 hour")
//    val words2Watermark = words2.withWatermark("t2", "2 hour")
//    val wordCounts: DataFrame = words.join(words2, words("title")=== words2("name"), joinType = "inner")

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      /**
        * complete： 完整的信息都会输出
        * update: 只输出更新的行信息，未更新的不输出，统计的原信息还存在
        * append：
        */
      .outputMode("append")
      .format("console")
//      .format("memory")
//      .trigger(Trigger.Continuous("1 second"))
      .start()

    /**
      * 本地测试
      *   数据接收 00.99 s
      *   处理完成 04.15 s
      */


    // avoid processing break
    query.awaitTermination()

    spark.wait()

  }

}
