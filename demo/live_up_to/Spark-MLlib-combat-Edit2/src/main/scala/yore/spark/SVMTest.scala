package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 支持向量机算法
 *
 * Created by Yore
 */
object SVMTest {

  def main(args: Array[String]): Unit = {
//    val line = "1 0 2.52078447201548 0 0 0 2.004684436494304 2.000347299268466 0 2.228387042742021 2.228387042742023 0 0 0 0 0 0"
//    val parts = line.split(" ")
//    println(parts.tail.mkString(" "))

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("SVMTest")
    val sc = new SparkContext(conf)

    val data = sc.textFile("demo/resources-data/sample_svm_data.txt")
    val parsedData = data.map(line => {
      val parts = line.split(" ")
      // tail 除第一个元素之外的后面所有元素
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
    })

    // 设置迭代次数，并进行训练
    val numIterations = 20
    val model = SVMWithSGD.train(parsedData, numIterations)
    //预测并统计分类错误的样本比例
    val labelAndPreds = parsedData.map(labeledPoint => {
      val prediction = model.predict(labeledPoint.features)
      (labeledPoint.label, prediction)
    })
    val trainErr = labelAndPreds.filter(rdd => rdd._1 != rdd._2)
      .count()
      .toDouble / parsedData.count()

    // Trainning Error = 0.38819875776397517
    println("Trainning Error = " + trainErr)

  }

}
