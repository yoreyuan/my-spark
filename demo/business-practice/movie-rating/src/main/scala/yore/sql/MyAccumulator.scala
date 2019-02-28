package yore.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by yore on 2019/1/16 17:17
  */
object MyAccumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("RDD_Movie_Users_Analyzer")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    // 累加器
    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1,2,3,4,6)).foreach(x => accum.add(x))

    println(accum.value)

    println("-" * 10)

    // 使用自定义的累加器
    val myaccum = new MyAccumulator2()
    sc.register(myaccum)
    sc.parallelize(Array("a", "b", "c", "d")).foreach(x => myaccum.add(x))
    println(myaccum.value)


    spark.stop()
  }

}

/**
  * 通过继承 AccumulatorV2 ，并指定输入为String， 输出为ArrayBuffer[String]
  *
  */
class MyAccumulator2 extends AccumulatorV2[String, ArrayBuffer[String]]{
  // 定义累加器结果的变量
  private var result = ArrayBuffer[String]()

  // 判断累加器当前的值是否为零值
  override def isZero: Boolean = this.result.size == 0

  // 新建累加器，并把result值赋给新的累加器
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccum = new MyAccumulator2
    newAccum.result = this.result
    newAccum
  }

  // 重置result的值
  override def reset(): Unit = this.result == new ArrayBuffer[String]()

  override def add(v: String): Unit = this.result += v

  // 合并两个累加器的结果
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    println(other.value)
    result.++=: (other.value)
  }

  override def value: ArrayBuffer[String] = this.result


}
