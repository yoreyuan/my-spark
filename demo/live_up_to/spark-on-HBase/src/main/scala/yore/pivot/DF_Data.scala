package yore.pivot

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import yore.SparkConfClass

/**
  *
  * Created by yore on 2019/10/12 11:15
  */
object DF_Data {
  val scc = new SparkConfClass()
  /**
    *  category|  quarter|  sales
    *  种类 | 季度 | 销售额
    */
  val store_sales = scc.getSc.parallelize(Array(
    "Books|Q4|4.66",
    "Books|Q1|1.58",
    "Books|Q3|2.84",
    "Books|Q2|1.50",
    "Women|Q1|1.41",
    "Women|Q2|1.36",
    "Women|Q3|2.54",
    "Women|Q4|4.16",
    "Music|Q1|1.50",
    "Music|Q2|1.44",
    "Music|Q3|2.66",
    "Music|Q4|4.36",
    "Children|Q1|1.54",
    "Children|Q2|1.46",
    "Children|Q3|2.74",
    "Children|Q4|4.51",
    "Sports|Q1|1.47",
    "Sports|Q2|1.40",
    "Sports|Q3|2.62",
    "Sports|Q4|4.30",
    "Shoes|Q1|1.51",
    "Shoes|Q2|1.48",
    "Shoes|Q3|2.68",
    "Shoes|Q4|4.46",
    "Jewelry|Q1|1.45",
    "Jewelry|Q2|1.39",
    "Jewelry|Q3|2.59",
    "Jewelry|Q4|4.25",
//    "null|Q1|0.04",
    "null|Q2|0.04",
//    "null|Q3|0.07",
    "null|Q4|0.13",
    "Electronics|Q1|1.56",
    "Electronics|Q2|1.49",
    "Electronics|Q3|2.77",
    "Electronics|Q4|4.57",
    "Home|Q1|1.57",
    "Home|Q2|1.51",
    "Home|Q3|2.79",
    "Home|Q4|4.60",
    "Men|Q1|1.60",
    "Men|Q2|1.54",
    "Men|Q3|2.86",
    "Men|Q4|4.71"
  ))
  val schemaStoreSales = StructType(
    "category|quarter".split("\\|")
      .map(column => StructField(column, StringType, true))
  ).add("sales", DoubleType, true)
  val store_salesRDDRows = store_sales.map(_.split("\\|"))
    .map(line => Row(
      line(0).trim,
      line(1).trim,
      line(2).trim.toDouble
    ))


  /**
    *  i_category|  Q1|  Q2|  Q3|  Q4
    *  种类 | 第一季度 | 第二季度 | 第三季度 | 第四季度
    */
  val movie = scc.getSc.parallelize(Array(
    "11| 1753|     4",
    "11| 1682|     1",
    "11|  216|     4|",
    "11| 2997|     4",
    "11| 1259|     3",
    "Shoes|1.51|1.48|2.68|4.46",
    "Jewelry|1.45|1.39|2.59|4.25",
    "null|0.04|0.04|0.07|0.13",
    "Electronics|1.56|1.49|2.77|4.57",
    "Home|1.57|1.51|2.79|4.60",
    "Men|1.60|1.54|2.86|4.71"
  ))


}
