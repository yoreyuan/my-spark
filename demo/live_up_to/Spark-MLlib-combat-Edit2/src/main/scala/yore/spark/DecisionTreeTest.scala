package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
// $example off$
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 决策树算法
 *
 * Created by Yore
 */
object DecisionTreeTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      // yore.spark.DecisionTreeTest$
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "demo/resources-data/sample_libsvm_data.txt")
    // 将数据切分为两份
    val splits:  Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.7, 0.3))
    splits.foreach(mapPartitionsRDD => {
      //mapPartitionsRDD.foreach(println(_))
      println(mapPartitionsRDD.count())
    })
    val (trainingData, testData) = (splits(0), splits(1))

    // 训练模型
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()  // 分类特征信息
    val impurity = "gini" //纯度计算方法：熵、基尼、方差
    val maxDepth = 5  // 树的最大高度
    val maxBins = 32  // 用于分裂特征的最大划分数量
    /*
     * .train()            //训练回归和分类模型
     * .trainClassifier()  //训练分类决策树模型
     * .trainRegressor()   //训练回归决策树模型
     */
    val model: DecisionTreeModel = DecisionTree
      .trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { labeledPoint =>{
      val prediction = model.predict(labeledPoint.features)
      (labeledPoint.label, prediction)
    }}
    val testErr = labelAndPreds
      .filter(r => r._1 != r._2)
      .count()
      .toDouble / testData.count()

    /*
Test Error = 0.07142857142857142
Learned classification tree model:
 DecisionTreeModel classifier of depth 1 with 3 nodes
  If (feature 406 <= 126.5)
   Predict: 0.0
  Else (feature 406 > 126.5)
   Predict: 1.0
     */
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    sc.stop()
  }
}
