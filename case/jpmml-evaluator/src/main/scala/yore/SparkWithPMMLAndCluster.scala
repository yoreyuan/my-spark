package yore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jpmml.evaluator.{DefaultVisitorBattery, LoadingModelEvaluatorBuilder}
import org.jpmml.evaluator.spark.TransformerBuilder

/**
  * 使用Spark整合PMML
  * 步骤：
  *   1.从hdfs读取PMML文件，构建模型
  *   2.构建DataFrame
  *   3.使用模型对构造的DataFrame数据进行预测
  *   4.把预测的结果写入HDFS
  * 参数：
  * args[0] 文件的path
  * args[1] 需要包含的列名
  * args[2] 结果的保存路径
  *
  */
object SparkWithPMMLAndCluster extends App {

  /*
     * 创建df对象
     */
  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    //      .setJars(Array("/opt/data/spark_t1-runtime-0.0.1-SNAPSHOT.jar"))
    .setAppName("spark_pmml")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  /*
   * 安全
   */
  if (args.length != 3) {
    System.err.println("Usage: scala " + SparkWithPMMLAndCluster.getClass + " <PMML file path> <Input filePath> <Output directory>")
    System.exit(-1)
  }
  val pmmlPath = args(0)
  val inputFilePath = args(1)
  val outputPath = args(2)

  var df = sparkSession.read.option("header", "true")
    .option("inferSchema", "true").csv(inputFilePath)
  df.cache()
  df.show()

  /*
   * 根据pmml文件，使用sparkmllib构建模型
   */
  val fs = FileSystem.get(new Configuration());
  val inputStream = fs.open(new Path(pmmlPath))
  val builder: LoadingModelEvaluatorBuilder = new LoadingModelEvaluatorBuilder()
    .setLocatable(false)
    .setVisitors(new DefaultVisitorBattery())
    .load(inputStream)

  //    val evaluator: ModelEvaluator[_ <: Model] = builder.build()
  val evaluator = builder.build()


  // Performing a self-check (duplicates as a warm-up)
  /* evaluator.verify();*/

  val modelBuilder = new TransformerBuilder(evaluator)
    .withTargetCols()
    .withOutputCols()
    .exploded(true)

  val transformer: Transformer = modelBuilder.build()

  /*
   * 预测
   */
  var resultDF: DataFrame = transformer.transform(df)
  resultDF.persist()

  /*
  结果处理
   */
  //写入hdfs
  //import sparkSession.implicits._
  print(888)
  /* resultDF.write.csv(outputPath)
   resultDF.rdd.saveAsTextFile(outputPath + "1")*/
  resultDF.printSchema()
  //resultDF.collect()
  resultDF.write.csv(outputPath)
  print(resultDF.rdd.getNumPartitions)

  // resultDF.write.mode(SaveMode.Overwrite).save(outputPath)

}
