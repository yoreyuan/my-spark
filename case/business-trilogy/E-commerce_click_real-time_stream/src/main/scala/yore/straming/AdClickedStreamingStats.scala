package yore.straming

import java.sql.ResultSet

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  *
  * Created by yore on 2019/3/6 09:51
  */
object AdClickedStreamingStats {

  def main(args: Array[String]): Unit = {

    // 设置日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster(PropertiesUtil.getPropString("spark.master"))
      .setAppName(PropertiesUtil.getPropString("spark.app.name"))
      // ！！必须设置，否则Kafka数据会报无法序列化的错误
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(PropertiesUtil.getPropInt("spark.streaming.durations.sec")))
    ssc.checkpoint(PropertiesUtil.getPropString("spark.checkout.dir"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getPropString("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> PropertiesUtil.getPropString("group.id"),
      "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
      "enable.auto.commit" -> (PropertiesUtil.getPropBoolean("enable.auto.commit"): java.lang.Boolean)
    )
    val topics = Array(PropertiesUtil.getPropString("kafka.topic.name"))

    val adClickedStreaming = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val filteredadClickedStreaming = adClickedStreaming.transform(rdd => {
      val blackListNames = ListBuffer[String]()
      val jdbcWrapper = JDBCWrapper.getInstance()

      def querycallBack(result : ResultSet): Unit = {
        while (result.next()){
          result.getString(1)
          blackListNames += result.getString(1)
        }
      }

      jdbcWrapper.doQuery("SELECT * FROM blacklisttable", null, querycallBack)

      val blackListTuple = ListBuffer[(String, Boolean)]()
      for(name <- blackListNames){
        val nameBoolean = (name, true)
        blackListTuple += nameBoolean
      }

      val blackListFromDB = blackListTuple
      val jsc = rdd.sparkContext
      val blackListRDD: RDD[(String, Boolean)] = jsc.parallelize(blackListFromDB)

      val rdd2Pair = rdd.map(t => {
        //
        /**
          * ConsumerRecord(
          *   topic = AdClicked,
          *   partition = 0,
          *   offset = 0,
          *   CreateTime = 1552355113636,
          *   serialized key size = -1,
          *   serialized value size = 55,
          *   headers = RecordHeaders( headers = [], isReadOnly = false),
          *   key = null,
          *   value = 1552355113636	192.168.112.250	9266	59	Liaoning	Shenyang)
          *  )
          */
//        println("$$$ \t" + t.value())
        val userID = t.value().split("\t")(2)
        (userID, t)
      })

      val joined = rdd2Pair.leftOuterJoin(blackListRDD)
      // (String, (ConsumerRecord[String, String], Option[Boolean]) )
      val result = joined.filter(v => {
        val optional = v._2._2
        if(optional.isDefined && optional.get){
          false
        }else{
          true
        }
      }).map(_._2._1)

      result
    });
    filteredadClickedStreaming.print()


    /**
      * 第四步
      */
    val pairs = filteredadClickedStreaming.map(t => {
      val splited = t.value().split("\t")

      val timestamp = splited(0)
      // yyyy-MM-dd
      val ip = splited(1)
      val userID = splited(2)
      val adID = splited(3)
      val province = splited(4)
      val city = splited(5)
      val clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_" + city

      (clickedRecord, 1L)
    })
    val adClickedUsers = pairs.reduceByKey(_ + _)


    val filteredClickInBatch = adClickedUsers.filter(v => {
      if(1 < v._2){
        // 更新黑名单的数据表
        false
      }else{
        true
      }
    })

    filteredClickInBatch.foreachRDD(rdd => {
      if(rdd.isEmpty()) rdd.foreachPartition(partition => {
        val userAdClickedList = ListBuffer[UserAdClicked]()
        while (partition.hasNext) {
          val record: Tuple2[String, Long] = partition.next
          val splited: Array[String] = record._1.split("_")
          val userAdClicked: UserAdClicked = new UserAdClicked

          userAdClicked.timestamp = splited(0)
          userAdClicked.ip = splited(1)
          userAdClicked.userID = splited(2)
          userAdClicked.adID = splited(3)
          userAdClicked.province = splited(4)
          userAdClicked.city = splited(5)

          userAdClickedList += userAdClicked
        }

        val inserting = ListBuffer[UserAdClicked]()
        val updating = ListBuffer[UserAdClicked]()
        val jdbcWrapper: JDBCWrapper = JDBCWrapper.getInstance()

        for(clicked <- userAdClickedList){
          def clickedquerycallBack(result: ResultSet): Unit = {
            while (result.next()){
              if((result.getRow - 1) != 0 ){
                val count = result.getLong(1)
                clicked.clickedCount = count
                updating += clicked
              }else{
                clicked.clickedCount = 0L
                inserting += clicked
              }
            }
          }
          jdbcWrapper.doQuery(
            "SELECT COUNT(1) FROM adclicked WHERE timestamp=? AND userID=? AND adID=?",
            Array(clicked.timestamp, clicked.userID, clicked.adID),
            clickedquerycallBack
          )
        }

        val insertParametersList = ListBuffer[ParamsList]()
        for(inserRecord <- inserting){
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = inserRecord.timestamp
          paramsListTmp.params2 = inserRecord.ip
          paramsListTmp.params3 = inserRecord.userID
          paramsListTmp.params4 = inserRecord.adID
          paramsListTmp.params5 = inserRecord.province
          paramsListTmp.params6 = inserRecord.city
          paramsListTmp.params10_Long = inserRecord.clickedCount
          paramsListTmp.params_Type = "adclickedInsert"
          insertParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("INSERT INTO adclicked VALUES(?,?,?,?,?,?,?)", insertParametersList)


        val updateParametersList = ListBuffer[ParamsList]()
        for(updateRecord <- updating){
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.ip
          paramsListTmp.params3 = updateRecord.userID
          paramsListTmp.params4 = updateRecord.adID
          paramsListTmp.params5 = updateRecord.province
          paramsListTmp.params6 = updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type = "adclickedUpdate"
          updateParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("UPDATE adclicked SET clickedCount=? WHERE timestamp=? AND ip=? AND userID=? AND adID=? AND province=? AND city=? ", updateParametersList)
      })
    })


    val blackListBasedOnHistory = filteredClickInBatch.filter(v => {
      val splited = v._1.split("_")
      val date = splited(0)
      val userID = splited(2)
      val adID = splited(3)

      val clickedCounttotalToday = 81
//      println(s" date=$date \t userID=$userID \t adID=$adID \t clickedCounttotalToday=$clickedCounttotalToday")
      /*if (clickedCounttotalToday > 50){
        return true
      } else{
        return false
      }*/
      true
    })

    // 对黑名单整个RDD进行去重操作
    // timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_" + city
    val blackListuserIDtBasedOnHistory = blackListBasedOnHistory.map(_._1.split("_")(2))
    val blackListUniqueuserIDtBasedOnHistory = blackListuserIDtBasedOnHistory.transform(_.distinct())

    blackListUniqueuserIDtBasedOnHistory.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val blackList = ListBuffer[ParamsList]()
        while (partition.hasNext){
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = partition.next()
          paramsListTmp.params_Type = "blacklisttableInsert"
          blackList += paramsListTmp
        }
        val jdbcWrapper: JDBCWrapper = JDBCWrapper.getInstance()
        jdbcWrapper.doBatch("INSERT INTO blacklisttable VALUES(?)", blackList)
      })
    })


    val filteredadClickedStreamingmappair = filteredadClickedStreaming.map(t => {
      val splited = t.value().split("\t")
      val timestamp = splited(0)
      val ip = splited(1)
      val userID = splited(2)
      val adID = splited(3)
      val province = splited(4)
      val city = splited(5)
      val clickedRecord = timestamp + "_" + adID + "_" + province + "_" + city

      (clickedRecord, 1L)
    })
    val updatefunc = (values: Seq[Long], state: Option[Long]) =>{
      Some[Long](values.sum + state.getOrElse(0L))
    }

    val updateStateByKeyDStream = filteredadClickedStreamingmappair.updateStateByKey(updatefunc)

    updateStateByKeyDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val adClickedList = ListBuffer[AdClicked]()
        while (partition.hasNext){
          val record = partition.next()
          val splited = record._1.split("_")
          val adClicked = new AdClicked
          adClicked.timestamp = splited(0)
          adClicked.adID = splited(1)
          adClicked.province = splited(2)
          adClicked.city = splited(3)
          adClicked.clickedCount = record._2

          adClickedList += adClicked
        }

        val inserting = ListBuffer[AdClicked]()
        val updating = ListBuffer[AdClicked]()
        val jdbcWrapper = JDBCWrapper.getInstance()

        for(clicked <- adClickedList){
          def adclickedquerycallBack(result: ResultSet) ={
            while (result.next()){
              if((result.getRow -1) != 0){
                val count = result.getLong(1)
                clicked.clickedCount = count
                updating += clicked
              }else{
                inserting += clicked
              }
            }
          }

          jdbcWrapper.doQuery(
            "SELECT COUNT(1) FROM adclickedcount WHERE timestamp=? AND adID=? AND province=? AND city=?",
            Array(clicked.timestamp, clicked.adID, clicked.province, clicked.city),
            adclickedquerycallBack
          )
        }

        val insertParametersList = new ListBuffer[ParamsList]()
        for(inserRecord <- inserting){
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = inserRecord.timestamp
          paramsListTmp.params2 = inserRecord.adID
          paramsListTmp.params3 = inserRecord.province
          paramsListTmp.params4 = inserRecord.city
          paramsListTmp.params10_Long = inserRecord.clickedCount
          paramsListTmp.params_Type = "adclickedcountInsert"
          insertParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("INSERT INTO adclickedcount VALUES(?,?,?,?,?)", insertParametersList)

        val updateParametersList = ListBuffer[ParamsList]()
        for(updateRecord <- updating){
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.adID
          paramsListTmp.params3 = updateRecord.province
          paramsListTmp.params4 = updateRecord.city
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type = "adclickedUpdate"
        }
        jdbcWrapper.doBatch("UPDATE adclickedcount set clickedCount=? WHERE timestamp=? AND adID=? AND province=? AND city=?", updateParametersList)
      })
    })


    /**
      * 计算每天各个省份的Top5排名的广告，
      * SELECT sum(clickedCount) clickSum,timestamp,province FROM adprovincetopn GROUP BY timestamp,province ORDER BY clickSum DESC LIMIT 5
      */
    val updateStateByKeyDStreamrdd = updateStateByKeyDStream.transform(rdd => {
      val rowRDD = rdd.map(t => {
        val splited = t._1.split("_")
        val timestamp = "2018-03-11"
        val adID = splited(1)
        val province = splited(2)
        val clickedRecord = timestamp + "_" + adID + "_" + province
        (clickedRecord, t._2)
      }).reduceByKey(_ + _).map(v => {
        val splited = v._1.split("_")
        val timestamp = "2018-03-11"
        val adID = splited(1)
        val province = splited(2)
        Row(timestamp, adID, province, v._2)
      })
      val structType = new StructType()
        .add("timestamp", DataTypes.StringType, true)
        .add("adID", DataTypes.StringType, true)
        .add("province", DataTypes.StringType, true)
        .add("clickedCount", DataTypes.LongType, true)

//      val hiveContext = new HiveContext(rdd.sparkContext)
      val spark = SparkSession.builder()
        .config(rdd.sparkContext.getConf)
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
      val df = spark.createDataFrame(rowRDD, structType)
      df.createOrReplaceTempView("topNTableSource")
      val sqlText = "SELECT timestamp,adID,province,clickedCount FROM (" +
        "SELECT timestamp,adID,province,clickedCount,row_number() " +
        "OVER(PARTITION BY province ORDER BY clickedCount DESC) rank FROM topNTableSource) subquery WHERE rank<=5";
      val result = spark.sql(sqlText)

      result.rdd
    })

    updateStateByKeyDStreamrdd.foreachRDD(rdd => {
      rdd.foreachPartition(t => {
        val adProvincetopN = ListBuffer[AdProvincetopN]()
        while (t.hasNext){
          val row = t.next()
          val item = new AdProvincetopN
          item.timestamp = row.getString(0)
          item.adID = row.getString(1)
          item.province = row.getString(2)
          item.clickedCount = row.getLong(3)
          adProvincetopN += item
        }
        val jdbcWrapper = JDBCWrapper.getInstance()
        val set = new mutable.HashSet[String]()
        for(itemTopn <- adProvincetopN){
          set += itemTopn.timestamp + "_" + itemTopn.province
        }

        val deleteParametersList = ListBuffer[ParamsList]()
        for(deleteRecord <- set){
          val splited = deleteRecord.split("_")
          val paramsListTmp = new ParamsList
          paramsListTmp.params1 = splited(0)
          paramsListTmp.params2 = splited(1)
          paramsListTmp.params_Type = "adprovincetopnDelete"
          deleteParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("DELETE FROM adprovincetopn WHERE timestamp=? AND province=?", deleteParametersList)

        val insertParametersList = ListBuffer[ParamsList]()
        for(updateRecord <- adProvincetopN){
          val paramsListTmp = new ParamsList()
          paramsListTmp.params1 = updateRecord.timestamp
          paramsListTmp.params2 = updateRecord.adID
          paramsListTmp.params3 = updateRecord.province
          paramsListTmp.params10_Long = updateRecord.clickedCount
          paramsListTmp.params_Type = "adprovincetopnInsert"
          insertParametersList += paramsListTmp
        }
        jdbcWrapper.doBatch("INSERT INTO adprovincetopn VALUES(?,?,?,?)", insertParametersList)
      })
    })



    val filteredadClickedStreamingPair = filteredadClickedStreaming.map(t =>{
      val splited = t.value().split("\t")
      val adID = splited(3)
      val time = splited(0)
      (time + "_" + adID, 1L)
    })
    filteredadClickedStreamingPair.reduceByKeyAndWindow(_ + _, _ + _, Seconds(1800), Seconds(60))
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val adTrend = ListBuffer[AdTrendStat]()
          while (partition.hasNext){
            val record = partition.next()
            val splited = record._1.split("_")
            val time = splited(0)
            val adID = splited(1)
            val clickedCount = record._2

            val adTrendStat = new AdTrendStat
            adTrendStat.adID = adID
            adTrendStat.clickedCount = clickedCount
            //TODO 获取年月日、小时、分钟
            adTrendStat._date = time
            adTrendStat._hour = time
            adTrendStat._minute = time
            adTrend += adTrendStat
          }
          val inserting = ListBuffer[AdTrendStat]()
          val updating = ListBuffer[AdTrendStat]()
          val jdbcWrapper = JDBCWrapper.getInstance()

          for(clicked <- adTrend){
            val adTrendCountHistory = new AdTrendCountHistory

            def adTrendquerycallBack(result: ResultSet) = {
              while (result.next()){
                if((result.getRow -1) != 0){
                  val count = result.getLong(1)
                  adTrendCountHistory.clickedCount = count
                  updating += clicked
                }else{
                  inserting += clicked
                }
              }
            }
            jdbcWrapper.doQuery(
              "SELECT count(1) FROM adclickedtrend WHERE date=? AND hour=? AND minute=? AND adID=?",
              Array(clicked._date, clicked._hour, clicked._minute, clicked.adID),
              adTrendquerycallBack
            )
          }

          val insertParametersList = ListBuffer[ParamsList]()
          for(inserRecord <- inserting){
            val paramsListTmp = new ParamsList
            paramsListTmp.params1 = inserRecord._date
            paramsListTmp.params2 = inserRecord._hour
            paramsListTmp.params3 = inserRecord._minute
            paramsListTmp.params4 = inserRecord.adID
            paramsListTmp.params10_Long = inserRecord.clickedCount
            paramsListTmp.params_Type = "adclickedtrendInsert"
            insertParametersList += paramsListTmp
          }
          jdbcWrapper.doBatch("INSERT INTO adclickedtrend VALUES(?,?,?,?,?)", insertParametersList)

          val updateParametersList = ListBuffer[ParamsList]()
          for(updateRecord <- updating){
            val paramsListTmp = new ParamsList
            paramsListTmp.params1 = updateRecord._date
            paramsListTmp.params2 = updateRecord._hour
            paramsListTmp.params3 = updateRecord._minute
            paramsListTmp.params4 = updateRecord.adID
            paramsListTmp.params10_Long = updateRecord.clickedCount
            paramsListTmp.params_Type = "adclickedtrendUpdate"
            updateParametersList += paramsListTmp
          }
          jdbcWrapper.doBatch("UPDATE adclickedtrend set clickedCount=? WHERE date=? AND hour=? AND minute=? AND adID=?", updateParametersList)
        })
      })

    ssc.start()
    ssc.awaitTermination()

  }

  def resultCallBack(result: ResultSet, blackListName: List[String]) : Unit = {

  }

}
