package yore.pivot

/**
  * Pivot将行转列
  *
  * Created by yore on 2019/10/12 11:07
  */
object PivotDemo {

  def main(args: Array[String]): Unit = {
    val store_salesFrame = DF_Data.scc.getSqlContext.createDataFrame(DF_Data.store_salesRDDRows, DF_Data.schemaStoreSales)
    store_salesFrame.show(20, false)

    //import scc.sqlContext.implicits.
    //使用Spark中的函数，例如 round、sum 等
    import org.apache.spark.sql.functions._

    store_salesFrame.groupBy("category")
      // 按照季度的顺序
      .pivot("quarter"/*, Seq("Q1", "Q2", "Q3", "Q4")*/)
      .agg(round(sum("sales"), 2))
//      .show(false)
        .createOrReplaceGlobalTempView("category")
//        .createTempView("category")

    // global_temp
    DF_Data.scc.getSqlContext
//      .sql("SELECT * FROM category")
//      .sql("SELECT category,round(Q1+Q2+Q3+Q4, 2) AS total FROM global_temp.category")
      .sql(
        """
          |SELECT category, Q1, Q2, Q3, Q4, ROUND(NVL(Q1, 0.0) + NVL(Q2, 0.0) + NVL(Q3, 0.0) + NVL(Q4, 0.0), 2) AS total
          |FROM global_temp.category
        """.stripMargin)
      .show(false)

  }

}
