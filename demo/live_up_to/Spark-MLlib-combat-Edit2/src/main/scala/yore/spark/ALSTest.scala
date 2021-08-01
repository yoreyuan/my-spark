package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于交替最小二乘法的协同过滤算法
 *
 * Created by Yore
 */
object ALSTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val data = sc.textFile("demo/resources-data/als_test.data")
    val ratings = data.map(_.split(",") match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toFloat)
    })
//    ratings.foreach(println(_))

    // 训练模型
    val rank = 1  // 模型中隐藏因子数
    val numIterations = 20  // 算法迭代次数
    val lambda = 0.01 // ALS 中的正则化参数
    val model = ALS.train(ratings, rank, numIterations, lambda)
//    val als = new ALS()
//      .setRank(rank)
//      .setMaxIter(numIterations)
//      .setAlpha(lambda)
////      .setLambda(lambda)
//      .setUserCol("user")
//      .setItemCol("item")
//      .setRatingCol("rating")
//    val model = als.fit(ratings)

    // 对模型进行评分
    val usersProducts = ratings.map {
      case Rating(user, product, rating) => (user, product)
    }
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rate) => ((user, product), rate)
    }
    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
//    ratesAndPreds.foreach(t => println(t))
    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) => math.pow(r1 - r2, 2)
    }.reduce(_ + _) / ratesAndPreds.count()
    println("Mean Squared Error = " + MSE)

    sc.stop()
  }

}
