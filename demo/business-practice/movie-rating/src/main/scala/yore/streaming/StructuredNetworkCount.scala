package yore.streaming

import org.apache.spark.sql.SparkSession

/**
  * nc -lk 19999
  *
  * Structured Streaming共有<a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html">三种输出模式</a>
  * ① Complete Mode - 整个更新的结果表将会写入外部存储器。由存储连接器决定如何处理整张表的写入。 聚合操作以及聚合之后的排序操作支持这种模式
  * ② Append Mode - 只有自上次触发执行后的结果中附加的新行会被写入外部存储器。这适用于结果表中的现有行不会更改的查询。 如select、where、map、flatMap、filter、join等操作
  * ③ Update Mode(vaaiable since Spark 2.1.1) - 只有自上次触发执行后的结果表中更新的行被写入外部存储器（不输出未更改的行）。
  *       注意这种模式和Complete Mode是不同的，在这种模式下仅输出自上次触发执行后的已经发生改变的行。如果查询不包含聚合，它等同于Append Mode
  *
  * <br/>
  * Created by yore on 2019/1/17 16:14
  */
object StructuredNetworkCount {

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    printf("%s\t%d\n", host, port)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkCount")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    //DF  Create DataFrame representing the stream of input lines form connection to host:port
    val lines = spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()

    // DF => DS  Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCount = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCount.writeStream
        .outputMode("complete")
        .format("console")
        .start()


    // avoid processing break
    query.awaitTermination()

    /*
    +-----+-----+
    |value|count|
    +-----+-----+
    |    l|    3|
    |    e|    2|
    |    o|    1|
    |    h|    2|
    +-----+-----+

    $ nc -lk 9999
    > y o r e
    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +-----+-----+
    |value|count|
    +-----+-----+
    |    l|    3|
    |    e|    3|
    |    o|    2|
    |    h|    2|
    |    y|    1|
    |    r|    1|
    +-----+-----+

     */

    //    spark.close()

  }



}
