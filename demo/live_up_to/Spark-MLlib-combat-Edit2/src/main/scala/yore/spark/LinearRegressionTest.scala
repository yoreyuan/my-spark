package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.LBFGS
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 线性回归算法
 *
 * Created by Yore
 */
object LinearRegressionTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      // yore.spark.DecisionTreeTest$
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val data = sc.textFile("demo/resources-data/lpsa.data")
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    })

    // 训练模型
    val numIterations = 100
    /*
     * LinearRegressionWithSGD 已过期，
     * 这里使用 LinearRegression 或者 LBFGS
     */
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    val valuesAndPreds = parsedData.map(labeledPoint => {
      val prediction = model.predict(labeledPoint.features)
      (labeledPoint.label, prediction)
    })
    val MSE = valuesAndPreds.map(t => math.pow(t._1 - t._2, 2))
      .reduce(_ + _) / valuesAndPreds.count()

    //training Mean Squared Error = 6.207597210613579
    println("training Mean Squared Error = " + MSE)

    sc.stop()
  }

}
