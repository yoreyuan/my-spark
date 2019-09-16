package yore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 为了防止sparkconf被序列化写的类
  * Created by yore on 2019/9/16 20:37
  */
class SparkConfClass() extends Serializable {

  @transient
  private val conf = new SparkConf().setAppName("UserPortraits").setMaster("local[4]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  @transient
  private val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  @transient
  private val sqlContext: SQLContext = new SQLContext(sc)

  def getSc: SparkContext = {
    sc
  }

  def getSqlContext: SQLContext = {
    sqlContext
  }

  def closeSc(): Unit = {

    sc.stop()
  }
}
