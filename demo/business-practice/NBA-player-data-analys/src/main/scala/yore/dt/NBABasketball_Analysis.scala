package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yore on 2019/2/27 17:48
  */
object NBABasketball_Analysis {

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[2]"
    var dataPath = "demo/business-practice/NBA-player-data-analys/src/main/resources/"

    if(args.length >0 ){
      masterUrl = args(0)
    }else if(args.length >1){
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl)
      .setAppName("E-commerce-interactive-analysis-system")
    val spark = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // demo/resources-data/ml-10M100K/ratings.dat is 252.82 MB;
    // demo/resources-data/ml-20m.zip is 189.50 MB;

  }


}
