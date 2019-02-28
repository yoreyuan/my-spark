package yore.dt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by yore on 2019/2/25 09:41
  */
object SysApp extends App {

  // 设置日志的输出级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  var masterUrl = "local[2]"
  var dataPath = "demo/resources-data/examples/"

  if(args.length >0 ){
    masterUrl = args(0)
  }else if(args.length >1){
    dataPath = args(1)
  }

  /**
    * Spark 1.6版本， HiveContext的创建
    */
//  val conf = new SparkConf()
//  conf.setAppName("Enterprise_personnel-management-system")
//  conf.setMaster(masterUrl)
//  val sc = new SparkContext(conf)
//  val hiveContext = new HiveContext(sc)
//  hiveContext.sql("use hive")
//  hiveContext.sql("DROP TABLE IF EXISTS scores")


  /**
    * Spark 1.6版本， SqlContext的创建
    */
//  val conf = new SparkConf()
//  conf.setAppName("Enterprise_personnel-management-system")
//  conf.setMaster(masterUrl)
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//  val userDataDF = sqlContext.createDataFrame(userDataRDDRow, structTypes)

  /**
    * Spark 1.6版本， StreamingContext的创建
    */
//  val conf = new SparkConf()
//    .setAppName("Enterprise_personnel-management-system")
//    .setMaster(masterUrl)   // "spark://192.168.189.1:7077"
////    .setJars(List())
//  val ssc = new StreamingContext(conf, Seconds(10))
//  ssc.checkpoint("usr/local/IMF_testdata/IMFcheckpoint114")


  /**
    * Spark 2.X 版本SparkSession
    */
  val conf = new SparkConf().setMaster(masterUrl)
    .setAppName("Enterprise_personnel-management-system")
  val spark = SparkSession.builder().config(conf)
    // 由于不同的文件前缀跨操作系统，为避免潜在的错误前缀问题，需要在启动SpareSession时指定spark.sql.warehouse.dir
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  //  spark.sql("show tables").show()
  /*spark.sql("CREATE TABLE IF NOT EXISTS src3(key INT, value STRING)")
  spark.sql("LOAD DATA LOCAL INPATH 'demo/resources-data/examples/kv2_y.txt' INTO TABLE src3")
  spark.sql("SELECT * FROM src3")
    .show()
  spark.sql("SELECT COUNT(*) FROM src3")
    .show()*/


  case class Person(name: String, age: Long)
  val persons = spark.read.json(dataPath + "people.json")
  val personsDS = persons.as[Person]
  personsDS.show()
  personsDS.printSchema()
  val personsDF = personsDS.toDF()

  case class PersonScores(n: String, score: Long)
  val personScoresDS = spark.read.json("demo/business-practice/Enterprise-personnel-management-system/src/main/resources/peopleScores.json")
      .as[PersonScores]
  val personScoresDF = personScoresDS.toDF()


  /**
    * 例1：使用map算子解析
    * 使用map算子将personDS 转换为Daataset[(String, Long)]类型，其中年龄值加100，生成姓名，年龄元组类型的DataSet
    */
  println("例1：使用map算子解析")
  personsDS.map(person => (person.name, person.age + 100L))
    .show()


  /**
    * 例2：使用flatMap 算子解析
    * 使用flatmap算子对personDS进行转换，模式匹配如果姓名为Andy，则年龄加上70，其他员工年龄加上30
    */
  println("例2：使用flatMap算子解析")
  personsDS.flatMap(person => person match {
    case Person(name, age) if(name == "Andy") => List((name, age + 70))
    case Person(name, age) => List((name, age + 30))
  }).show()


  /**
    * 例3：mapPartitions 算子解析
    * 使用mapPartition算子对personDS执行mapPartitions算子，对每个分区的记录集合，循环遍历Person是记录集，
    * 如Person是有元素，则将姓名、年龄值奖赏1000组成的元组值加入到ArrayBuffer列表，最终返回result。iterator的值
    */
  println("例3：mapPartitions算子解析")
  personsDS.repartition(2).mapPartitions(persons => {
    val result = ArrayBuffer[(String, Long)]()
    println("### ")
    while (persons.hasNext){
      val person = persons.next()
      println("***\t" + person)
      result += ((person.name, person.age + 1000))
    }
    result.iterator
  }).show()


  /**
    * 例4：dropDuplicate 算子解析
    * 使用dropDuplicate算子删除重复元素，例如，使用personDS.dropDuplicates("name").show删除姓名中国重复员工的记录
    */
  println("例4：dropDuplicate算子解析")
  personsDS.dropDuplicates("name")
    .show()
  personsDS.distinct().show()


  /**
    * 例5：repartition 算子解析
    * 使用repartition算子重新设置personDS的分区数，
    */
  println("例5：repartition算子解析")
  val repartitionDS = personsDS.repartition(4)
  println("repartition: " + repartitionDS.rdd.partitions.size)


  /**
    * 例6：coalesce 算子解析
    */
  println("例6：coalesce算子解析")
  val coalesced = repartitionDS.coalesce(2)
  println("coalesce: " + coalesced.rdd.partitions.size)


  /**
    * 例7：sort 算子解析
    * 使用sort算子对personDS按照年龄进行降序排列
    */
  println("例7：sort算子解析")
  repartitionDS.sort($"age".desc).show()


  /**
    * 例8：join 算子解析
    * 使用join算子将数据集根据提供的表达式进行关联
    * +---+-------+-------+-----+
    * |age|   name|      n|score|
    * +---+-------+-------+-----+
    * | 16|Michael|Michael|   88|
    * | 30|   Andy|   Andy|  100|
    * | 19| Justin| Justin|   89|
    * | 29| Justin| Justin|   89|
    * | 46|Michael|Michael|   88|
    * +---+-------+-------+-----+
    */
  println("例8：join算子关联企业人员信息、企业人员分数评分信息")
  personsDS.join(personScoresDS, $"name" === $"n").show


  /**
    * 例9：joinWith 算子解析
    * 使用joinWith算子对数据集进行内关联，当关联的姓名相等时，返回一个tuple2键值对，格式分别为( (年龄，姓名), (姓名，评分) )
    *
    * +-------------+-------------+
    * |           _1|           _2|
    * +-------------+-------------+
    * |[16, Michael]|[Michael, 88]|
    * |   [30, Andy]|  [Andy, 100]|
    * | [19, Justin]| [Justin, 89]|
    * | [29, Justin]| [Justin, 89]|
    * |[46, Michael]|[Michael, 88]|
    * +-------------+-------------+
    */
  println("例9：使用joinWith算子关联企业人员信息、企业人员分数评分信息")
  personsDS.joinWith(personScoresDS, $"name" === $"n").show()


  /**
    * 例10：randomSplit算子解析
    * 使用randomSplit算子对普洱搜DS进行随机切分。
    * randomSplit的参数weights表示权重，传入的两个值将会切成两个Dataset[Person]，
    * 把原来的DataSet[Person]按照权重10，20随机划分到两个Dataset[Person]中，权重高的Dataset[Person]划分的几率就大
    * +---+------+
    * |age|  name|
    * +---+------+
    * | 29|Justin|
    * +---+------+
    *
    * +---+-------+
    * |age|   name|
    * +---+-------+
    * | 16|Michael|
    * | 19| Justin|
    * | 30|   Andy|
    * | 46|Michael|
    * +---+-------+
    */
  println("例10：使用randomSplit算子进行随机切分")
  personsDS.randomSplit(Array(10, 20))
    .foreach(dataset => dataset.show())


  /**
    * 例11：sample算子解析
    * 使用sample算子对personDS进行随机采样，
    * 第一个参数true标识有放回的去抽样，false标识没有放回的抽样；
    * 第二个参数为采样率在0 ~ 1
    *
    * +---+------+
    * |age|  name|
    * +---+------+
    * | 29|Justin|
    * +---+------+
    */
  println("例11：使用sample算子进行随机采样")
  personsDS.sample(false, 0.5).show()


  /**
    * 例12： select算子解析
    * 使用select算子对年龄进行显示
    */
  println("例12：使用select算子选择列")
  personsDS.select("name").show()


  /**
    * 例13： groupBy 算子解析
    * 使用groupBy算子对姓名、年龄进行分组，统计分组后的计数
    */
  println("例13：使用groupBy算子进行分组")
  val personsDSGrouped = personsDS.groupBy($"name"/*, $"age"*/)
    .count
    .show()

  /**
    * 例14： agg 算子解析
    * 使用agg算子concat内置函数，将姓名、年龄连接在一起，成为单个字符长列
    * +-------+---+-----------------+
    * |   name|age|concat(name, age)|
    * +-------+---+-----------------+
    * | Justin| 29|         Justin29|
    * |   Andy| 30|           Andy30|
    * |Michael| 16|        Michael16|
    * |Michael| 46|        Michael46|
    * | Justin| 19|         Justin19|
    * +-------+---+-----------------+
    */
//  import spark.sqlContext.implicits._
//  import spark.implicits._
  import org.apache.spark.sql.functions._
  println("例14：agg算子concat内置函数，将姓名、年龄连接在一起，成为单个字符长列")
  personsDS.groupBy($"name", $"age")
    .agg(concat($"name", $"age"))
    .show()


  /**
    * 例15： col 算子解析
    * 使用col算子选择personDS的姓名列以及personScoresDS的姓名列，
    * 如相等，则进行joinWith关联
    */
  println("例15：使用col算子选择列，")
  personsDS.joinWith(
    personScoresDS,
    personsDS.col("name") === personScoresDS.col("n")
  ).show()


  /**
    * 例16： collect_list、collect_set函数解析
    * 将personDS按照姓名分组，然后调用agg方法，collect_list是分组以后的姓名集合，
    * collect_set是去重以后的姓名集合。结果中无重复元素
    * collect_list函数结果中包含重复元素；
    * +-------+------------------+-----------------+
    * |   name|collect_list(name)|collect_set(name)|
    * +-------+------------------+-----------------+
    * |Michael|[Michael, Michael]|        [Michael]|
    * |   Andy|            [Andy]|           [Andy]|
    * | Justin|  [Justin, Justin]|         [Justin]|
    * +-------+------------------+-----------------+
    */
  println("例16：collect_list、collect_set函数解析")
  personsDS.groupBy($"name")
    .agg(collect_list($"name"), collect_set($"name"))
    .show()


  /**
    * 例17： sum、avg、min、count、countDistinct、mean、current_date函数解析
    * 将personDS按照姓名进行分组，调用agg方法，分别执行年龄求和、平均年龄、最大年龄、最小年龄、唯一年龄计数、平均年龄、当前时间等函数
    * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
    * |   name|sum(age)|avg(age)|max(age)|min(age)|count(age)|count(DISTINCT age)|avg(age)|current_date()|
    * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
    * |Michael|      62|    31.0|      46|      16|         2|                  2|    31.0|    2019-02-26|
    * |   Andy|      30|    30.0|      30|      30|         1|                  1|    30.0|    2019-02-26|
    * | Justin|      48|    24.0|      29|      19|         2|                  2|    24.0|    2019-02-26|
    * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
    */
  println("例17：sum、avg、min、count、countDistinct、mean、current_date函数解析")
  personsDS.groupBy($"name")
    .agg(sum($"age"), avg($"age"), max($"age"), min($"age"), count($"age"), countDistinct($"age"), mean($"age"), current_date())
    .show()



}
