package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * K-Means 聚类算法
 *
 * Created by Yore
 */
object KMeansExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      // yore.spark.DecisionTreeTest$
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)


    val data = sc.textFile("demo/resources-data/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
//      .cache()

    // 训练模型
    val numClusters = 2
    val numIterations = 15
    val model = KMeans.train(parsedData, numClusters, numIterations)
    for (elem <- model.clusterCenters) {
      println(s" $elem")
    }

    // 评估模型
    val WSSSE = model.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 使用模型对单条特征进行预测
    val predictVal1 = model.predict(Vectors.dense("0.1,0.2,0.3".split(",").map(_.toDouble)))
    println(s"Predict_1: $predictVal1")
    val predictVal2 = model.predict(Vectors.dense("7.1,8.2,9.3".split(",").map(_.toDouble)))
    println(s"Predict_1: $predictVal2")

    data.map(line => {
      val lineVectors = Vectors.dense(line.split(" ").map(_.toDouble))
      val prediction = model.predict(lineVectors)
      //println(s"$line belongs to cluster $prediction")
      s"$line belongs to cluster $prediction"
    }).foreach(println(_))
//      .saveAsTextFile("demo/live_up_to/Spark-MLlib-combat-Edit2/src/main/resources/kmeans_result")


    sc.stop()
  }

}
