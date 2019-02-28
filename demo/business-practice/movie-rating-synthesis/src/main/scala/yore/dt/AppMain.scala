package yore.dt

import org.apache.spark.{SparkConf, SparkContext}
import yore.dt.Movie_Users_Analyzer_RDD.masterUrl

/**
  *
  * Created by yore on 2019/2/21 15:26
  */
object AppMain extends App{

  val conf = new SparkConf().setMaster("local[1]")
    .setAppName("Movie_Users_Analyzer")

  /*val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext*/

  val sc = new SparkContext(conf)


  val rdd1 = sc.makeRDD(Array(("1","Spark"),("2","Hadoop"),("3","Scala"),("4","Java")),2)
  val rdd2 = sc.makeRDD(Array(("1",30),("2",15),("3",25),("5",10)),2)

  rdd1.join(rdd2).foreach(println)


}
