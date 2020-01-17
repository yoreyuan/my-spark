package yore

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yore on 2019/9/23 10:19
  */
object shc_demo {

  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("shc demo")
    .set("hbase.zookeeper.quorum", "cdh6")
    .set("hbase.zookeeper.property.clientPort", "2181")
    .set("zookeeper.znode.parent", "/hbase")
    .set(TableInputFormat.INPUT_TABLE, "demo")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  /**
    * The above defines a schema for a HBase table with name as table1, row key as key and a number of columns (col1-col8).
    * Note that the rowkey also has to be defined in details as a column (col0), which has a specific cf (rowkey).
    */
  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"demo", "tableCoder":"PrimitiveType"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"emp", "col":"empno", "type":"string"},
                   |"col2":{"cf":"emp", "col":"ename", "type":"string"},
                   |"col3":{"cf":"emp", "col":"job", "type":"string"},
                   |"col4":{"cf":"emp", "col":"mgr", "type":"int"},
                   |"col5":{"cf":"emp", "col":"hiredata", "type":"string"},
                   |"col6":{"cf":"emp", "col":"sal", "type":"double"},
                   |"col7":{"cf":"emp", "col":"comm", "type":"string"},
                   |"col8":{"cf":"dept", "col":"comm", "type":"string"},
                   |"col9":{"cf":"dept", "col":"loc", "type":"string"}
                   |}
                   |}""".stripMargin


  def main(args: Array[String]): Unit = {

//    val df = withCatalog(catalog)
//    df.createOrReplaceTempView("demo")
//    spark.sql("select * from demo").show()
//    spark.stop()

    writeHBase



  }

  def withCatalog(cat: String): DataFrame = {

    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def writeHBase(): Unit ={
    import sqlContext.implicits._
    val data = (0 to 3).map(DemoRecord(_))
    sc.parallelize(data).toDF()
//      .show()
      .write
      //.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")) // 如果表不存在时， 5代表Region的个数
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))  // 如果表存在时
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()

  }


}

case class DemoRecord(
     col0: String,  // rowkey
     col1: String,  // empno
     col2: String,  // ename
     col3: String,  // job
     col4: Int,     // mgr
     col5: String,  // hiredata
     col6: Double,  // sal
     col7: String,  // comm
     col8: String,   // comm
     col9: String   // loc
)

object DemoRecord{
  val enames = Array("SMITH", "ALLEN", "WARD", "JONES", "MARTIN", "BLAKE", "CLARK", "SCOTT", "KING", "TURNER", "ADAMS", "JAMES", "FORD", "MILLER")
  val jobs = Array("CLERK", "SALESMAN", "MANAGER", "ANALYST", "PRESIDENT")
  val dnames = Array("ACCOUNTING", "RESEARCH", "SALES", "OPERATIONS")
  val locs = Array("NEW YORK", "DALLAS", "CHICAGO", "BOSTON")

  // 用于模拟数据
  def apply(i: Int): DemoRecord = {
    DemoRecord(
      i.toString, "325"+i, enames(i%14).toLowerCase, jobs(i%5).toLowerCase, i,
      dateStr(), 1000*i, (200*i).toString, dnames(i%4).toLowerCase, locs(i%4).toLowerCase
    )
  }
  def dateStr():String = {
    val sf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sf.format(new java.util.Date())

  }
}
