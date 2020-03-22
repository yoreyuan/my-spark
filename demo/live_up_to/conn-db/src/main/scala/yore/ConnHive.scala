package yore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialects

/**
  *
  * Created by yore on 2020/3/19 19:14
  */
object ConnHive {

  def main(args: Array[String]): Unit = {
    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)


//    def fun01_spark1_6(): Unit ={
//      import org.apache.log4j.{Logger,Level}
//      import org.apache.spark.{SparkConf, SparkContext}
//      val conf = new SparkConf().setAppName("spark-hive").setMaster("local[1]")
//      val sc = new SparkContext(conf)
//      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//      sqlContext.sql("use adl")
//      val df = sqlContext.sql("select count(*) from b010_business_contract")
//      df.show()
//      sc.stop()
//    }


    def fun01_spark2_x(): Unit ={
      /**
        * Spark 2.0之后引入SparkSession，封装了SparkContext和SQLContext，
        * 并且会在builder的getOrCreate方法中判断是否有符合要求的SparkSession存在，有则使用，没有则创建。
        *
        * 如果使用 Hive 需要在 SparkSession 时开启 enableHiveSupport
        *
        */
      val spark = SparkSession.builder()
        .appName("spark_conn_hive")
        .master("local[1]")
        .enableHiveSupport()
        .getOrCreate()
      // 屏蔽 sparkContext 的日志
      spark.sparkContext.setLogLevel("WARN");

      val hiveDF = spark.sql("SELECT * FROM hive_test.data_test")

      hiveDF.show(false)

      spark.stop()
      spark.close()
    }
//    fun01_spark2_x;


    /**
      * 采用 option 方式 通过 jdbc 连接 Hive
      *
      * <pre>
      *   会发现只能获取 STRING 类型的表字段。且表的值都是 字段名
      *
      *     root
      *     |-- one.name: string (nullable = true)
      *
      *     +--------+
      *     |one.name|
      *     +--------+
      *     |one.name|
      *     |one.name|
      *     +--------+
      *
      *     root
      *     |-- w_test.id: long (nullable = true)
      *     |-- w_test.name: string (nullable = true)
      *     |-- w_test.country: string (nullable = true)
      *
      *     Caused by: java.lang.NumberFormatException: For input string: "w_test.id"
      *
      *   解决
      *     ①手动实现一个 HiveSqlDialect 并实现 JdbcDialect
      *     ②注册生效 JdbcDialects.registerDialect(HiveSqlDialect)
      *
      * </pre>
      *
      */
    def fun02_spark2_x(): Unit ={
      val spark = SparkSession.builder()
        .appName("spark_conn_hive")
        .master("local[1]")
//        .enableHiveSupport()
        .getOrCreate()
      // 屏蔽 sparkContext 的日志
      spark.sparkContext.setLogLevel("WARN");

      JdbcDialects.registerDialect(yore.HiveSqlDialect)

//      println(yore.HiveSqlDialect.quoteIdentifier("w_test.name"))
//      import spark.implicits._
//      import spark.sqlContext.implicits._
      val hiveDF = spark.read.format("jdbc")
        .option(JDBCOptions.JDBC_DRIVER_CLASS, "org.apache.hive.jdbc.HiveDriver")
        .option(JDBCOptions.JDBC_URL, "jdbc:hive2://cdh3:10000/default")
        .option("user", "hive")
        .option("password", "hive")
        .option(JDBCOptions.JDBC_TABLE_NAME, "w_test")
//        .option(JDBCOptions.JDBC_QUERY_STRING, "SELECT * FROM w_test")
        .option(JDBCOptions.JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES, "id bigint, name string, country string")
        .load()
        .withColumnRenamed("w_test.id", "id")
        .withColumnRenamed("w_test.name", "name")
        .withColumnRenamed("w_test.country", "country")

      hiveDF.printSchema()

      hiveDF.selectExpr("cast(id as string)", "name")
        .limit(3)
        .show()

      spark.stop()
      spark.close()
    }
    fun02_spark2_x;


  }

}
