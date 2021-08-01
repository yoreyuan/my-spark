package yore.comm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex.{Match, MatchIterator}

/**
  *
  * Created by yore on 2019/3/19 18:02
  */
object LogQuery {

  val exampleApacheLogs : ListBuffer[String] = ListBuffer[String](
    """10.10.10.10 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg HTTP/1.1" 304 315 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)" "UD-1" - "image/jepg" "whatever" 0.350 "-" - "" 265 923 934 "" 62.24.11.25 images.com 1368492167 - whatup""",
    """10.10.10.110 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg HTTP/1.1" 304 315 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)" "UD-1" - "image/jepg" "whatever" 0.350 "-" - "" 265 923 934 "" 62.24.11.25 images.com 1368492167 - whatup""",
    """10.10.10.10 - "FRED" [18/Jan/2013:18:02:37 +1100] "GET http://images.com/2013/Generic.jpg HTTP/1.1" 304 306 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)" "UD-1" - "image/jepg" "whatever" 0.352 "-" - "" 256 977 988 "" 0 73.23.2.15 images.com 1358492557 - whatup"""
  )

  val apacheLogRegex = """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] \"(.+?)\" (\d{3}) ([\d\-]+) \"([^\"]+)\" \"([^\"]+)\" .*""".r


  def extractKey(line: String) ={
    val m: MatchIterator = apacheLogRegex.findAllIn(line)
    if(m.hasNext){
      val ip:String = m.group(1)
      val user:String = m.group(3)
      val query:String = m.group(5)

      user match {
        case "-" => (null, null, null)
        case _ => (ip, user, query)
      }
    }
  }

  def extractStats(line: String): Stats = {
    val m: MatchIterator = apacheLogRegex.findAllIn(line)
    if(m.hasNext){
      val bytes: Int = m.group(7).toInt
      Stats(1, bytes)
    }else{
      Stats(1, 0)
    }
  }


  /**
    * Spark主程序代码
    * @param args
    */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setAppName(PropertiesUtil.getPropString("spark.app.name"))
      .setMaster(PropertiesUtil.getPropString("spark.master"))
    val sc = SparkContext.getOrCreate(sparkConf)

    val dataSet: RDD[String] = if(args.length == 1) sc.textFile(args(0)) else sc.parallelize(exampleApacheLogs)

    val extracted/*: RDD[((String, String, String), Stats)]*/ = dataSet.map(line =>{
      (extractKey(line), extractStats(line))
    })

    val counts = extracted.reduceByKey((s1, s2) => s1.merge(s2))

    val output = counts.collect

    for(t <- output){
      println(t._1 + "\t" + t._2)
    }


    sc.stop()
  }

}


class Stats(count1: Int, numBytes2: Int) extends Serializable{
  val count: Int = count1
  val numBytes: Int = numBytes2

  override def toString = s"Stats($count, $numBytes)"

  def merge(other: Stats): Stats = {
    return Stats(this.count + other.count, this.numBytes + other.numBytes)
  }
}
object Stats{
  var stats: Stats = _
  def apply(count1: Int, numBytes2: Int): Stats = stats match {
    case null => new Stats(count1, numBytes2)
    case _ => stats
  }
}




