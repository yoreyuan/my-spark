package yore.streaming

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}
import yore.streaming.indicator_sql._

/**
  * nc -l 19999
  *
  *
  * Created by yore on 2019/4/10 10:07
  */
object IndicatorCalculation {

  private var host: String = "localhost"
  private var port: Int = 19999

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Structure Streaming Performance")
      .master("local")
      .config("variable spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    spark.udf.register("adddate", MyUDF.addDateEval _)
    spark.udf.register("curdate", MyUDF.curdateEval _)
    spark.udf.register("datediff", MyUDF.dateDiffEval _)
    spark.udf.register("last_day", MyUDF.lastDayEval _)
    spark.udf.register("my_date_format", MyUDF.DataFormatEval _)



    /**
    * Socket测试
    */
//    socketTest(spark)




    /**
    * yqlb2 到 bllm3a2 测试
    */
//    yqlb2ToBllm3a2Test(spark)


    /**
    * pfje 到 pfjea6 测试
    */
    pfjeToPfjea6(spark)


    spark.wait()

  }

  /**
    * yqlb2 到 bllm3a2 测试
    *
    */
  def yqlb2ToBllm3a2Test(spark: SparkSession)={
    import spark.implicits._

    val p: DataFrame = spark.readStream
      .format("socket")
      .option("host", host).option("port", 19990)
      .load()
      .as[String]
      .map(
        _.split("")
          .map(a => b090_cf_direct_repayment_plan("1.1".toDouble, "1.2".toDouble, "p1", getDate("2019-01-01"), "term"))
      ).toDF()
    p.createOrReplaceTempView("b090_cf_direct_repayment_plan")


    val r: DataFrame = spark.readStream
      .format("socket")
      .option("host", host).option("port", 19991)
      .load()
      .as[String]
      .map(
      _.split("")
      .map(a => b090_cf_direct_repayment_record("p1", "term", getDate("2019-04-09")) )
      ).toDF()
    r.createOrReplaceTempView("b090_cf_direct_repayment_record")


    val query = spark.sql(SQL("pfje"))
      .writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()

    query.awaitTermination()

  }


  /**
    * pfje 到 pfjea6 测试
    *
    */
  def pfjeToPfjea6(spark: SparkSession)={
    import spark.implicits._

    val c: DataFrame = spark.readStream
      .format("socket")
      .option("host", host).option("port", 19990)
      .load()
      .as[String]
      .map(_.split(" "))
      .map(a => prplclaim("c_001", a(0).toDouble, getDate(a(1)) ) )
      .toDF()
    c.printSchema()
    c.createOrReplaceTempView("prplclaim")


    val t: DataFrame = spark.readStream
      .format("socket")
      .option("host", host).option("port", 19991)
      .load()
      .as[String]
      .map(_.split(" "))
      .map(a => t_ec_uw_baseinfo("c_001", "03" , new java.sql.Timestamp(getDate().getTime)))
      .toDF()
    t.printSchema()
    t.createOrReplaceTempView("t_ec_uw_baseinfo")



    val table_id = "pfjea6"
    //
    val sql: String = SQL(table_id).replace("sum(sumclaim)", "sum(sumclaim),event_time")
    println(sql)
    val query = spark.sql(sql)
      .withWatermark("t.event_time", "3 seconds")
      //      .map((table_id, _))
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()

    query.awaitTermination()

  }





  // -----------------------

  def socketTest(spark: SparkSession)={
    import spark.implicits._

    val lines: DataFrame = spark.readStream
        .format("socket")
        .option("host", host).option("port", 19990)
        .load()

    val words = lines.as[String]
      .flatMap(line =>{
        println(s"* \t " + line)
        line.split(" ")
      }).map(Word(_, 1)).toDF()

    words.createOrReplaceTempView("words")

    // select w,sum(c) from words group by w
    val query = spark.sql("select w,sum(c) from words group by w")
      .writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()

    query.awaitTermination()

  }











  def getDate()={
    new Date(System.currentTimeMillis())
  }

  /**
    *
    * @param dataStr yyyy-MM-dd
    * @return
    */
  def getDate(dataStr: String)={
    val s = new SimpleDateFormat("yyyy-MM-dd")
    new Date(s.parse(dataStr).getTime)
  }

  def getRandomDeci()={
    val randomNum: Double = scala.util.Random.nextDouble()*10
    randomNum.formatted("%.2f").toDouble
  }


}
