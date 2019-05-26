package yore.streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.StreamingContext

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
  *
  *    outputMode(OutputMode.Append)有如下值：
  *       complete： 完整的信息都会输出。没有聚合时不能使用
  *       update: 只输出更新的行信息，未更新的不输出，统计的原信息还存在
  *       append：
  *
  * </pre>
  *
  *
  * Created by yore on 2019/4/8 11:00
  */
object ss_performance_c1 {

  private var host: String = "localhost"
  private var port: Int = 19999

  /**
    * val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    *
    * streaming application 的持续运行，checkpoint 数据占用的存储空间会不断变大。需要小心设置checkpoint 的时间间隔。
    * 设置得越小，checkpoint 次数会越多，占用空间会越大；如果设置越大，会导致恢复时丢失的数据和进度越多。
    * 一般推荐设置为 batch duration 的5~10倍。
    *
    */
  //  def functionToCreateContext(): StreamingContext = {
//    val ssc = new StreamingContext()
//    val lines = ssc.socketTextStream()
//      ...
//    ssc.checkpoint(checkpointDirectory)
//    ssc
//
//  }

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Structure Streaming Performance")
      .master("local")
      .getOrCreate()



    /**
      * 数据源为 Socket
      *   we hava a cat
      *   we hava a pig
      *   I love Beijing
      */
//    socketTest(spark)


    /*if (args.length < 3) {
      System.err.println("Usage: WindowOnEventTimeDemo <host地址> <端口号>" +
        " <窗口长度/秒> [<窗口滑动距离/秒>]")
      System.exit(1)
    }*/
    /**
      * 基于窗口的单词方法
      *   I have a cat
      *   I love Beijing
      *   I love Shanghai
      */
//    windowTest(spark, args)


    /**
      * 从目录下读取 文件（CSV文件）作为数据源，
      *   在如下路径下下创建一个文件，如  ss_1.csv
      *     yore,18
      *     yuan,20
      *
      *   然后再创建一个新的文件   ss_2.csv
      *     Tom,21
      *     Lucy,24
      *     Jack,49
      *
      */
    val pathStr = "demo/live_up_to/Spark-MLib-combat-Edit2/src/main/resources";
//    sourceCSVTest(spark, pathStr)


    /**
      * 流式 DataFrame/Dataset 注册临时表
      * sql执行
      * join
      */
    tempViewTest(spark)




  }


  /**
    * Socket source
    * @param spark SparkSession
    */
  def socketTest(spark: SparkSession) = {
    //    import org.apache.spark.sql.functions._
    //    import spark.sqlContext.implicits._
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

    // Generate running word count。结果表. 按单词计算词频
    val wordCounts=words.groupBy("value").count()

//    wordCounts.createOrReplaceTempView("table_name")
//    spark.sql("select count(*) from table_name")

    /**
      * 选择（Selection）、投射（Projection）
      *
      *   I hava a cat and a pig
      *   I love a cat and a pig
      *   I hava a car and a dog
      *
      */
    val operationDF = wordCounts
      .select("value")
      .where("count>3")


    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      /**
        * complete： 完整的信息都会输出。没有聚合时不能使用
        * update: 只输出更新的行信息，未更新的不输出，统计的原信息还存在
        * append：
        */
      .outputMode("complete")
      .format("console")
      .start()


    // avoid processing break
    query.awaitTermination()

  }


  /**
    * 窗口操作（基于窗口的单词 代码）
    *   在滑动的事件-时间窗口上的聚合对于结构化流是简单的，非常类似于分组聚合
    *
    * @param spark SparkSession
    */
  case class TimeWord(word: String, timestamp: Timestamp)
  def windowTest(spark: SparkSession, args: Array[String]) = {
    //    import org.apache.spark.sql.functions._
    //    import spark.sqlContext.implicits._
    import spark.implicits._

    //窗口长度
    val windowSize = /*args(2).toInt*/ 10
    //窗口滑动距离，默认是窗口长度
    val slideSize = /*if (args.length == 3) windowSize else args(3).toInt*/ 5
    //窗口滑动距离应当小于或等于窗口长度
    if (slideSize > windowSize) {
      System.err.println("窗口滑动距离应当小于或等于窗口长度")
    }

    //以秒为单位
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    // 创建DataFrame
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)//添加时间戳
      .load()


    val words = lines.as[(String, Timestamp)]
      .flatMap(line => line._1.split(" ").map(word => TimeWord(word, line._2))).toDF()

    // 计数
//    val windowedCounts = words.groupBy(
//      window($"timestamp", windowDuration, slideDuration),
//      $"word"
//    ).count()
//      .orderBy("window")

    /* ---- 水印 ---- */
    val late = windowDuration
    val windowedCounts = words.withWatermark("timestamp", late)
      .groupBy(window($"timestamp", windowDuration, slideDuration), $"word")
      .count().orderBy("window")


    // 查询
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }


  /**
    * 从目录下读取 文件（CSV文件）作为数据源，
    *   在滑动的事件-时间窗口上的聚合对于结构化流是简单的，非常类似于分组聚合
    *
    * @param spark SparkSession
    */
  def sourceCSVTest(spark: SparkSession, pathStr: String) = {
    //    import org.apache.spark.sql.functions._
    //    import spark.sqlContext.implicits._
    import spark.implicits._

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")

    val csvDF = spark
      .readStream
      .option("sep", ",")//name和age分隔符
      .schema(userSchema)      // Specify schema of the csv files
      .csv(pathStr)    // // Equivalent to format("csv").load("/path/to/directory")

    // Returns True for DataFrames that have streaming sources
    println("=================DataFrames中已经有流=================|"+csvDF.isStreaming +"|")

    //打印csvDF模式
    csvDF.printSchema
    //执行结构化查询，将结果写入控制台console，输出模式为Append
    val query = csvDF.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()


    query.awaitTermination()
  }


  /**
    * 流式 DataFrame/Dataset 注册临时表
    * sql执行
    * join
    *
    * Kafka 数据源
    *   UserID Age
    *
    * Socket 数据源
    *   UserID Gender
    *
    * @param spark SparkSession
    */
  case class User(UserID: String, Gender: String, loadtime: String)
  case class Info(UserID: String, Age: String, loadtime: String)
  def tempViewTest(spark: SparkSession) = {
    //    import org.apache.spark.sql.functions._
    //    import spark.sqlContext.implicits._
    import spark.implicits._

    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    /*val words = socketDF.as[String]
      .flatMap(line =>{
        println(s"* \t " + line)
        line.split(" ")
      })*/

    val words = socketDF.as[String]
      .map(_.split(" "))
      .filter(_.size>1)
      .map(wordsArr => User(wordsArr(0), wordsArr(1), System.currentTimeMillis().toString) ).toDF()
    words.createOrReplaceTempView("wordsTable")


    /**
      * Structured Streaming + Kafka Integration
      * http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
      */
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "cdh3:9092,cdh4:9092,cdh5:9092")
      .option("subscribe", "example")
      //      .option("startingOffsets", "earliest")
      //      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "100")
      .load()

    /*val words2 = kafkaDF.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")  // 没有转换则是字节数组
      .as[(String, String)]
      .flatMap(_._2.split(" "))
      .toDF("name")*/

    val words2 = kafkaDF.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")  // 没有转换则是字节数组
      .as[(String, String)]
      .map(_._2.split(" "))
      .filter(_.size>1)
      .map(wordsArr => Info(wordsArr(0), wordsArr(1), System.currentTimeMillis().toString) ).toDF()
    words2.createOrReplaceTempView("words2Table")


    /*val query = spark.sql("select value,count(1) from wordsTable group by value").writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()*/


    /**
      * 3 女
      * 3 18
      */
    var sql = "select * from wordsTable"
    sql = "select * from wordsTable t1 inner join words2Table t2 on t1.UserID=t2.UserID"
    val query = spark.sql(sql).writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()

    query.awaitTermination()


  }



}
