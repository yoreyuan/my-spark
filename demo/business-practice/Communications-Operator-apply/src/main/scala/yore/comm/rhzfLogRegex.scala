package yore.comm

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * 融合支付日志正则表达式解析分析
  *
  * Created by yore on 2019/3/19 17:14
  */
object rhzfLogRegex {


  val RHZF_EXAMPLE_DATA: ListBuffer[String] = ListBuffer(
    """2016-12-15 00:02:55,619 [ERROR] [MQBillPaymentBusiness.java] : 282 -- 流水号:20**60；MQ账单支付业务的时间： >>> Payment >>> [419]ms""",
    """2016-12-15 00:02:59,228 [ERROR] [MQBillCheckBusiness.java] : 233 -- 流水号:20**66；MQ校验订单业务的时间: >>> CheckBill >>> [147]ms""",
    """2016-12-15 00:02:59,525 [ERROR] [MQBillPaymentBusiness.java] : 282 -- 流水号:20**67；MQ账单支付业务的时间: >>> Payment >>> [287]ms""",
    """2016-12-15 00:03:21,417 [DEBUG] [MQQueryInvoiceBusiness.java] : 98 -- 流水号:15c*59de61576,queryInvoice response:<soap:Envelope xmlns:soap="http://*/soap/envelope/"><soap:Body><ns2:queryInvoiceResponse xmlns:ns2="http://*.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>*t.com.exception.ApplicationException: 账户不存在或当前不是有效状态""",
    """2016-12-15 00:03:32,855 [DEBUG] [MQQueryInvoiceBusiness.java] : 98 -- 流水号:1156*4f70,queryInvoice response:<soap:Envelope xmlns:soap="http://*/envelope/"><soap:Body><ns2:query InvoiceResponse xmlns:ns2="http://*.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>*.exception.ApplicationException: 账户不存在或当前不是有效状态""",
    """2016-12-15 00:03:33,183 [ERROR] [MQQueryAccIdBusiness.java] : 53 -- 流水号:1008*5643；MQ条码号查询业务的时间: >>> QueryAccExternalId >>> [247]ms""",
    """2016-12-15 21:31:52,015 [WARN] [MQQueryAccountNoBusiness.java] : 278 -- 流水号:1008@@27,queryAccountNo response:<soap:Envelope xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:query queryAccountNoResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>java.lang.Exception: 设备号对应账号为空""",
    """2017-01-15 17:41:21,654 [ERROR] [MQBillCheckBusiness.java] : 233 -- 流水号:20170@@636；MQ校验订单业务的时间: >>> CheckBill >>> [554]ms""",
    """2017-01-15 17:47:56,242 [ERROR] [MQAuthOperateBusiness.java] : 45 -- 去UAM认证的业务的时间: >>> Auth >>> [31]ms""",
    """2017-01-15 17:47:58,430 [DEBUG] [MQQueryInvoiceBusiness.java] : 498 -- 流水号:372@@8698,queryInvoice response:<soap:Envelope xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:query InvoiceResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode><errDesc>com.@@.exception.ApplicationException: 账户不存在或当前不是有效状态"""
  )

  /**
    * 正则表达式解析场景：
    *
    * 日志分析：提取融合支付到UAM的日志
    * 日志样本：
    *   2017-01-15 17:47:56,242 [ERROR] [MQAuthOperateBusiness.java] : 45 -- 去UAM认证的业务的时间: >>> Auth >>> [31]ms
    */
  val RHZF_UAM_TIME_REGEX = """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+) >>> (\S+) >>> (\S+)](\S+)$""".r

  /**
    * 日志分析：提取融合支付到计费账务系统销账单失败返回码
    * 日志样本：
    *   2017-01-15 17:47:58,430 [DEBUG] [MQQueryInvoiceBusiness.java] : 498 -- 流水号:372@@8698,queryInvoice response:<soap:Envelope
    *   xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:query InvoiceResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode>
    *   <errDesc>com.@@.exception.ApplicationException: 账户不存在或当前不是有效状态
    */
  val RHZF_MQ_QueryInvoice_ErrCode_REGEX = """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+),(\S+) (\S+) (\S+) (.*)<response><head><errCode>([0-9]*)</errCode><errDesc>(.*): (\S+)$""".r

  /**
    * 日志分析：提取融合支付到计费账务系统MQ校验订单业务的时间
    * 日志样本：
    *   2017-01-15 17:41:21,654 [ERROR] [MQBillCheckBusiness.java] : 233 -- 流水号:20170@@636；MQ校验订单业务的时间: >>> CheckBill >>> [554]ms
    */
  val RHZF_MQ_CHECK_ORDER_TIME_REGEX = """^(\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+) >>> (\S+) >>> (\S+) (\S+)](\S+)$""".r

  /**
    * 日志分析：提取融合支付到计费账务系统销账失败返回码
    * 日志样本：
    *   2016-12-15 21:31:52,015 [WARN] [MQQueryAccountNoBusiness.java] : 278 -- 流水号:1008@@27,queryAccountNo response:<soap:Envelope
    *   xmlns:soap="http://@@/soap/envelope/"><soap:Body><ns2:query queryAccountNoResponse xmlns:ns2="http://@@.sh.cn/"><response><head><errCode>00000001</errCode>
    *   <errDesc>java.lang.Exception: 设备号对应账号为空
    */
  val RHZF_MQ_queryAccountNo_ErrCode_REGEX = """^(\S+) (\S+) (\S+) (\S+) (\S+) : (\S+) -- (\S+):(\S+),(\S+) (\S+) (\S+) (.*)<response><head><errCode>([0-9]*)</errCode><errDesc>(.*): (\S+)$""".r

  def main_(args: Array[String]): Unit = {
    val index: Int = 0
    /**
      *   RHZF_UAM_TIME_REGEX
      *   RHZF_MQ_QueryInvoice_ErrCode_REGEX
      *   RHZF_MQ_CHECK_ORDER_TIME_REGEX
      *   RHZF_MQ_queryAccountNo_ErrCode_REGEX
      */
    val m = RHZF_UAM_TIME_REGEX.findAllIn(RHZF_EXAMPLE_DATA(index))
    println(RHZF_EXAMPLE_DATA(index))
//    println(m.size)
//    println(m.hasNext)
    if(m.hasNext){
      var resu = "①$10\n②$11\n③$12\n④$13\n⑤$14\n⑥$15\n⑦$16\n⑧$17\n⑨$18\n⑩$19\n⑪$20\n⑫$21\n⑬$22\n⑭$23\n⑮$24\n"
      for(i <- 10 until 25-5){
        resu = resu.replace("$"+i, m.group(i-10))
      }
      println(resu)
    }

  }

  /**
    * Spark主程序代码
    *
    * rhzfAlipayLogAnalysis
    * @param args
    */
  def main(args: Array[String]): Unit = {

    var masterUrl = PropertiesUtil.getPropString("spark.master")
    var dataPath = ""
    val outputPath = "demo/business-practice/Communications-Operator-apply/src/main/resources"

    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length >0 ){
      masterUrl = args(0)
    }else if(args.length>1){
      dataPath = args(1)
    }

    val sparkConf = new SparkConf()
      .setAppName(PropertiesUtil.getPropString("spark.app.name"))
      .setMaster(masterUrl)
//    val sc = SparkContext.getOrCreate(sparkConf)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    /*val linesConvertToGbkRDD: RDD[String] = sc.hadoopFile(dataPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 4)
      .map(line => {
        // 转码
        new String(line._2.getBytes, 0, line._2.getLength, "GBK")
      })*/
    val dataSet: RDD[String] = if(StringUtils.isNotBlank(dataPath)) sc.textFile(dataPath) else sc.parallelize(RHZF_EXAMPLE_DATA)
    val linesConvertToGbkRDD = dataSet.map(line => {
//      new String(line.getBytes, 0, line.length, "UTF-8")
      line
    })



    val lineRegexd: RDD[String] = linesConvertToGbkRDD.map(rhzfline => {
      rhzfline match {
        case RHZF_UAM_TIME_REGEX(uamLog1, uamLog2, uamLog3, uamLog4, uamLog5, uamLog6, uamLog7, uamLog8, uamLog9) => rhzfline
        case RHZF_MQ_QueryInvoice_ErrCode_REGEX(mqLog1, mqLog2, mqLog3, mqLog4, mqLog5, mqLog6, mqLog7, mqLog8, mqLog9, mqLog10, mqLog11, mqLog12, mqLog13, mqLog14) => rhzfline
        case RHZF_MQ_CHECK_ORDER_TIME_REGEX(mqTimeLog1, mqTimeLog2, mqTimeLog3, mqTimeLog4, mqTimeLog5, mqTimeLog6, mqTimeLog7, mqTimeLog8, mqTimeLog9, mqTimeLog10, mqTimeLog11) => rhzfline
        case RHZF_MQ_queryAccountNo_ErrCode_REGEX(mqWarnLog1, mqWarnLog2, mqWarnLog3, mqWarnLog4, mqWarnLog5, mqWarnLog6, mqWarnLog7, mqWarnLog8, mqWarnLog9, mqWarnLog10, mqWarnLog11, mqWarnLog12, mqWarnLog13, mqWarnLog14, mqWarnLog15) => rhzfline
        case _ => {
          println("+ " + rhzfline)
          "MatcheIsNull"
        }
      }
    })
//    lineRegexd.collect().foreach(s => println(s))

    //过滤掉 MatcheIsNull字符
    val linedFilterd: RDD[String] = lineRegexd.filter(!_.contains("MatcheIsNull"))
    // 合并为一个分区，保存数据时就会生成一个数据文件，方便后续加载到Splunk进行展示
    val outPath = new Path(outputPath + "/result")
    outPath.getFileSystem(new Configuration).delete(outPath, true)
    linedFilterd.coalesce(1).saveAsTextFile(outputPath + "/result")


    sc.stop()

  }

}
