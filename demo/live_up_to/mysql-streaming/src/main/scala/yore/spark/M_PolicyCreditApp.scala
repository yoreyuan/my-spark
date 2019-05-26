package yore.spark

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  *
  * Created by yore on 2019/3/18 15:11
  */
object M_PolicyCreditApp {

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster(PropertiesUtil.getPropString("spark.master"))
      .setAppName(PropertiesUtil.getPropString("spark.app.name"))
      // ！！必须设置，否则Kafka数据会报无法序列化的错误
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//        .set("log4j.rootCategory", "ERROR, console")
    //如果环境中已经配置HADOOP_HOME则可以不用设置hadoop.home.dir
//    System.setProperty("hadoop.home.dir", "/Users/yoreyuan/soft/hadoop-2.9.2")

    val ssc = new StreamingContext(conf,  Seconds(PropertiesUtil.getPropInt("spark.streaming.durations.sec").toLong))
    ssc.sparkContext.setLogLevel("ERROR")

//    ssc.checkpoint(PropertiesUtil.getPropString("spark.checkout.dir"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getPropString("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> PropertiesUtil.getPropString("group.id"),
      "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
      "enable.auto.commit" -> (PropertiesUtil.getPropBoolean("enable.auto.commit"): java.lang.Boolean)
    )
    val topics = Array(PropertiesUtil.getPropString("kafka.topic.name"))

    val kafkaStreaming = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    kafkaStreaming.map[JSONObject](line => { // str转成JSONObject
      println("$$$\t" + line.value())
      JSON.parseObject(line.value)
    }).filter(jsonObj =>{   // 过滤掉非 INSERT和UPDATE的数据
      if(null == jsonObj || !"canal_test".equals(jsonObj.getString("database")) ){
        false
      }else{
        val chType = jsonObj.getString("type")
        if("INSERT".equals(chType) || "UPDATE".equals(chType)){
          true
        }else{
          false
        }
      }
    }).flatMap[(JSONObject, JSONObject)](jsonObj => {   // 将改变前和改变后的数据转成Tuple
      var oldJsonArr: JSONArray = jsonObj.getJSONArray("old")
      val dataJsonArr: JSONArray = jsonObj.getJSONArray("data")
      if("INSERT".equals(jsonObj.getString("type"))){
        oldJsonArr = new JSONArray()
        val oldJsonObj2 = new JSONObject()
        oldJsonObj2.put("mor_rate", "0")
        oldJsonArr.add(oldJsonObj2)
      }

      val result = ListBuffer[(JSONObject, JSONObject)]()
//      val listTuple = List[(JSONObject, JSONObject)]()
//      listTuple.:+(jsonTuple)

      for(i <- 0 until oldJsonArr.size ) {
        val jsonTuple = (oldJsonArr.getJSONObject(i), dataJsonArr.getJSONObject(i))
        result += jsonTuple
      }
      result
    }).filter(t => {  // 过滤状态不为1的数据，和mor_rate没有改变的数据
      val policyStatus = t._2.getString("policy_status")
      if(null != policyStatus && "1".equals(policyStatus) && null!= t._1.getString("mor_rate")){
        true
      }else{
        false
      }
    }).map(t => {
      val p_num = t._2.getString("p_num")
      val nowMorRate = t._2.getString("mor_rate").toDouble
      val chMorRate = nowMorRate - t._1.getDouble("mor_rate")
      val riskRank = gainRiskRank(nowMorRate)

      // p_num, risk_rank, mor_rate, ch_mor_rate, load_time
      (p_num, riskRank, nowMorRate, chMorRate, new java.util.Date)
    }).foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        val paramsList = ListBuffer[ParamsList]()
        val jdbcWrapper = JDBCWrapper.getInstance()
        while (p.hasNext){
          val record = p.next()
          val paramsListTmp = new ParamsList
          paramsListTmp.p_num = record._1
          paramsListTmp.risk_rank = record._2
          paramsListTmp.mor_rate = record._3
          paramsListTmp.ch_mor_rate = record._4
          paramsListTmp.load_time = record._5
          paramsListTmp.params_Type = "real_risk"
          paramsList += paramsListTmp
        }
        /**
          * VALUES(p_num, risk_rank, mor_rate, ch_mor_rate, load_time)
          */
        val insertNum = jdbcWrapper.doBatch("INSERT INTO real_risk VALUES(?,?,?,?,?)", paramsList)
        println("INSERT TABLE real_risk: " + insertNum.mkString("\t"))
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }



  def gainRiskRank(rate: Double): String = {
    var result = ""
    if(rate>=0.75 && rate<0.8){
      result = "R1"
    }else if(rate >=0.80 && rate<=1){
      result = "R2"
    }else{
      result = "G1"
    }
    result
  }

}

/**
  * 结果表对应的参数实体对象
  */
class ParamsList extends Serializable{
  var p_num: String = _
  var risk_rank: String = _
  var mor_rate: Double = _
  var ch_mor_rate: Double = _
  var load_time:java.util.Date = _
  var params_Type : String = _
  override def toString = s"ParamsList($p_num, $risk_rank, $mor_rate, $ch_mor_rate, $load_time)"
}
