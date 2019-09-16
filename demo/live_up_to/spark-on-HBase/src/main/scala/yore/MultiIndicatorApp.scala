package yore

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  *
  * Created by yore on 2019/9/16 20:36
  */
object MultiIndicatorApp {

  val scc = new SparkConfClass()
  var day: String = _

  def main(args: Array[String]): Unit = {

    val df1 = scc.getSc.parallelize(Array(
      "001|a|*1|12",
      "002|b|*2|18",
      "003|c|*3|20")
    )

    val schemaForUsers = StructType(
      "id|name|password".split("\\|")
        .map(column => StructField(column, StringType, true))
    ).add("age", IntegerType, true)
    // 然后我们把每一条数据变成以Row为单位的数据
    val usersRDDRows = df1.map(_.split("\\|"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim.toInt))
    //基于SparkSession的createDataFrame方法，结合Row和StructType的元数据信息
    // 基于RDD创建DataFrame，这是RDD就有了源数据信息的描述
    val usersDataFrame = scc.getSqlContext.createDataFrame(usersRDDRows, schemaForUsers)

    // false，不截断字符串
//    usersDataFrame.show(false)

    // 带计算的指标
    val f = Array("id", "N", "age>=18 as f1")

    usersDataFrame.selectExpr("id", "name as N", "age")
      .selectExpr(
//        "id", "N", "age>=18 as f1"
//        for(s <- f) yield s + ","
        f: _*
      )
      .show(false)


  }



  def getDate:String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
  }

}


