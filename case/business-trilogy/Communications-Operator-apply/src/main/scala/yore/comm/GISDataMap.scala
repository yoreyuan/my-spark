package yore.comm

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.{DecimalFormat, SimpleDateFormat}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Spark 在光宽用户流量热力分布GIS系统中的综合应用案例
  *
  * Created by yore on 2019/3/21 14:41
  */
object GISDataMap {

  def main(args: Array[String]): Unit = {

    val warehouselocation = "/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("GISDataMap")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouselocation)
      .getOrCreate()

    val schemaString2 = "OLTPORT2,FirstTime,LastTime,I_Mbps,IA_Mbps,O_Mbps,OA_Mbps"

    var fields2: Seq[StructField] = List[StructField]()
    for(columnName <- schemaString2.split(",")){
      // 分别规定String和Double类型
      if(columnName == "I_Mbps" || columnName == "IA_Mbps" || columnName == "O_Mbps" || columnName == "OA_Mbps"){
        fields2 = fields2 :+  StructField(columnName, DoubleType, true)
      }else {
        fields2 = fields2 :+ StructField(columnName, StringType, true)
      }
    }
    val structType_ponData = StructType.apply(fields2)

    // 结合数据给出要处理的时间，格式yyyyMMdd
    val fileName = "hdfs:///cdh2/OltPonPort_FlowData_" + args(0) + "*"
    var ponData = spark.read.textFile(fileName).rdd

    /**
      * 找出相关格式，并进行关联
      */
    val ponData_rdd = ponData.map(x => {
      val array = x.split(",")
      val port = array(3).split("/")
      // 使用replace去除空格
      val oltPortTemp = array(2) + "|0" + port(1) + "|0" + port(2)
      val oltPort = oltPortTemp.replace(" ", "")
      val firstTime = array(4).split("-")(0).replace(" ", "")
      val lastTime = array(4).split("-")(1)

      Row(oltPort, firstTime, lastTime, array(5).toDouble, array(6).toDouble, array(7).toDouble, array(8).toDouble)
    })

    val df_ponDF = spark.createDataFrame(ponData_rdd, structType_ponData)

    /**
      * 读取Hive相关的表，进行定期操作
      * olt_port, ad_location
      */
    val df_gisData = spark.sql("SELECT * FROM OLTPORT_LOCATION")

    /**
      * 关联
      */
    val result = df_ponDF.join(df_gisData, df_ponDF.col("OLTPORT2") === df_gisData.col("olt_port"))
      .drop("OLTPORT2")

    result.show(20)

    val printRdd = result.rdd.map(x => {
      var result = x.apply(0)
      for(i <- 1 until  x.length){
        result = result + "," + x.apply(i)
      }
      result
    })


    /**
      * 针对以上情况进行处理
      * 再次清洗，给出JD_WD
      */
    result.foreachPartition(p => {
      val url = "jdbc:oracle:thin:@10.100.100.109:1521:ORCL"
      val user = "yore"
      val password = "123"
      var conn: Connection = null
      var ps: PreparedStatement = null

      /**
        * 插入Oracle数据库的DX_LLXXB表
        *
        * 序号，开始时间，结束时间，入向峰值，入向平均值，出向峰值，出向平均值，olt光网设备端口号，经纬度位置，温度信息/前，olt经纬度(按:分割)，经度，纬度，当前时间，开始时间，转换后的经度，转换后的纬度
        */
      val sql = "INSERT INTO DX_LLXXB(xh, firsttime, lasttime, i_mbps, ia_mbps, o_mbps, oa_mbps, oltport, olmc, l_local, jd, wd, drrq, ywrg, jdl, wdl) " +
        "VALUES(SEQ_DXLL.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

      try{
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)

        ps = conn.prepareStatement(sql)

        p.foreach(row => {
          // 计算经纬度
          val jd = row.getAs[String]("ad_location").split(":")(0)
          val wd = row.getAs[String]("ad_location").split(":")(1)

          // 转换后的经纬度
          val jdl = jd.toDouble * 20037508.34 / 180
          var wdl = math.log(Math.tan((90 + wd.toDouble) * math.Pi / 360)) / (math.Pi/180)
          wdl = wdl * 20037508.34 / 180

          // 当前的时间
          var now = new java.util.Date
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val DRRQ = sdf.format(now)
          val YWRQ = row.getAs[String]("FirstTime")

          // 对SQL进行复制
          ps.setString(1, row.getAs[String]("FirstTime"))
          ps.setString(2, row.getAs[String]("LastTime"))
          ps.setDouble(3, row.getAs[Double]("I_Mbps"))
          ps.setDouble(4, row.getAs[Double]("IA_Mbps"))
          ps.setDouble(5, row.getAs[Double]("O_Mbps"))
          ps.setDouble(6, row.getAs[Double]("OA_Mbps"))
          ps.setString(7, row.getAs[String]("olt_port"))
          // OLT的名称
          ps.setString(8, row.getAs[String]("olt_port").split("/")(0))
          ps.setString(9, row.getAs[String]("ad_location"))
          ps.setString(10, jd)
          ps.setString(11, wd)
          ps.setString(12, DRRQ)
          ps.setString(13, YWRQ)

          val df = new DecimalFormat("#.0000000")
          ps.setString(14, df.format(jdl))
          ps.setString(15, df.format(wdl))

          ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()
      }catch {
        case e: Exception => e.printStackTrace()
      }finally {
        if(ps != null){
          ps.close()
        }
        if(conn != null){}
        conn.close()
      }
    })

  }

}