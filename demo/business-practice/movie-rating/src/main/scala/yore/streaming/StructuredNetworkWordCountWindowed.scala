package yore.streaming

import java.sql.Timestamp

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.window

/**
  *
  * Created by yore on 2019/1/18 11:16
  */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    // "1 minute", "10 seconds"
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    printf("%s\n%s\n", windowDuration, slideDuration)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)]
      .flatMap(line =>
        line._1.split(" ").map(word => (word, line._2)
      )
    ).toDF("word", "timestamp")

    /*
    root
 |-- window: struct (nullable = true)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- word: string (nullable = true)
 |-- count: long (nullable = false)
     */

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
          window($"timestamp", windowDuration, slideDuration), $"word"
        ).count()
        .orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    val writerStream = windowedCounts.writeStream.outputMode("complete")
    // 输出到文件中，Append模式支持这种方式。这种方式自带容错机制
//    writerStream.format("parquet").start()
    // 输出到控制台，用于调试。Append模式和Complete模式支持这种方式。
//    writerStream.format("console").start()
    // 以table的形式输出到内存，可以在之后的程序中使用 spark.sql(select * from table).show来对结果进行处理。这种方式同样使用于调试。Append模式和Complete模式支持这种方式
//    writerStream.format("memory").queryName("table").start()
    /*writerStream.foreach(
      new ForeachWriter[Row]{
        override def open(partitionId: Long, /*epochId*/version: Long): Boolean = {
          // Open connection
          partitionId > version
          /*
          partitionId 是分区ID，
          version是重复数据删除的唯一ID标识。用于数据失败时重复数据的删除，
                当从失败中恢复时，一些数据可能生成多次，但他们具有相同的版本。
          open方法用于处理Executor分区的初始化（如打开连接启动事物等）。
                如果此方法发现使用的partitionId和version这个分区已经处理后，可以返回false，以跳过进一步的数据处理，
                但close仍然要求清理资源
           */
        }


        override def process(value: Nothing): Unit = {
          // Write string to connection

          /*
          调用此方法处理Executor中的数据。此方法值在open时调用，返回true
           */
          println("Write string to connection")
        }

        override def close(errorOrNull: Throwable): Unit = {
          // Close the connection
          /*
          停止执行Executor一个分区中的新数据时调用，保证被调用open时返回true，或者返回false。

          在下列情况下，close不会被调用：
            JVM崩溃，没有抛出异常Throwable；
            open方法抛出异常Throwable

           */
        }
      }
    ).start()*/
    // kafka
//    writerStream.format("kafka").option("topic", "someTopic").start()

    // 为了让程序在重启后可以可以接着上次的执行结果继续执行，需要设置检查点
    writerStream.outputMode("complete")
        .option("checkpointLocation", "path/to/HDFS/dir")
        .format("memory")
        .start()

    query.awaitTermination()

  }

}

class MyForeachWriter[T] extends ForeachWriter with Serializable{
  override def open(partitionId: Long, epochId: Long): Boolean = {
    // Open connection
    partitionId > epochId
  }

  override def process(value: Nothing): Unit = {
    // Write string to connection
  }

  override def close(errorOrNull: Throwable): Unit = {
    // Close the connection
  }
}
