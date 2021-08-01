package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 贝叶斯分类算法
 *
 * Created by Yore
 */
object NaiveBayesTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("NaiveBayesTest")
    val sc = new SparkContext(conf)

    val data = sc.textFile("demo/live_up_to/Spark-MLlib-combat-Edit2/src/main/resources/nb_traindata.txt")
    val trainData = data.map(line => {
      // 0,1 0 0
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble) ))
    })
    val testData = sc.textFile("demo/live_up_to/Spark-MLlib-combat-Edit2/src/main/resources/nb_testdata.txt")
      .map(line => {
        val parts = line.split(" ")
        Vectors.dense(parts.map(_.toDouble))
      })

    // 训练模型
    val model = NaiveBayes.train(trainData, 1.0)
    // 使用模型， 对单条特征进行预测
    val predict1: Double = model.predict(Vectors.dense("0,0,3".split(",").map(_.toDouble)))
    println("predict1: " + predict1)

    // 使用模型对批量特征进行预测，并将结果保存到文件
    testData.map(rdd => {
      val predict2 = model.predict(rdd)
      rdd + " belons to class " + predict2
    }).saveAsTextFile("demo/live_up_to/Spark-MLlib-combat-Edit2/src/main/resources/nb_result")

  }

}
