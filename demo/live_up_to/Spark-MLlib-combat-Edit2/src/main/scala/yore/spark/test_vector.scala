package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * 稀疏型数据集 -- spares
  * 密集型数据集 -- dense
  *
  * Created by yore on 2019/3/25 17:38
  */
object test_vector {

  def main0(args: Array[String]): Unit = {
    // 密集型数据集
    val vd: Vector = Vectors.dense(2, 0, 6)
    println(vd(2))

    //
    /**
      *
      * 稀疏型数据集
      *
      * (向量大小:要求大于等于输入数值的个数，向量索引：增加，与索引长度相同)
      */
    val vs: Vector = Vectors.sparse(9, Array(0, 1, 5, 6), Array(9, 5, 2, 7))
    println(vs(6))

  }


  /**
    * 4.1.3 向量标签的使用
    */
  def main1(args: Array[String]): Unit = {

    val vd: Vector = Vectors.dense(2, 0, 6)
    // 对密集向量建立标记点
    val pos = LabeledPoint(1, vd)
    // 打印标记点内容数据
    println(pos.features)
    // 打印既定标记
    println(pos.label)

    val vs : Vector = Vectors.sparse(4, Array(0,1,2,3), Array(9, 5, 2, 7))
    val neg = LabeledPoint(2, vs)
    println(neg.features)
    println(neg.label)

  }


  /**
    * 从文件中读取数据获取固定格式的数据集，作为矩阵
    *
    * 1 1:2 2:3 3:4
    * 2 1:5 2:8 3:9
    * 1 1:7 2:6 3:7
    * 1 1:3 2:2 3:1
    */
  def main2(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("testLabeledPoint2")

    val sc = new SparkContext(conf)
    val mu = MLUtils.loadLibSVMFile(sc, "demo/live_up_to/Spark-MLlib-combat-Edit2/src/main/resources/loadLibSVMFile.txt")
    mu.foreach(println)

  }


  /**
    * 4.1.4 本地矩阵的使用
    */
  def main3(args: Array[String]): Unit = {
    //创建一个分布式矩阵
    val mx = Matrices.dense(2, 3, Array(1,2,3,4,5,6))
    println(mx)
  }


  /**
    * 4.1.5 分布式矩阵的使用
    */
  def main4(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建环境变量
    val conf = new SparkConf()
      //设置本地化处理
      .setMaster("local")
      //设定名称
      .setAppName("testRowMatrix")

    //创建环境变量实例
    val sc = new SparkContext(conf)
    //创建RDD文件路径
    val rdd = sc.textFile("demo/resources-data/examples/data/mllib/als/test.data")

    // 1. 行矩阵
//    rowMatrixTest(rdd)

    // 2. 带有行索引的行矩阵
//    indexedRowMatrixTest(rdd)

    // 3. 坐标矩阵和块矩阵
    coordinateRowMatrixTest(rdd)

  }

  def rowMatrixTest(line_rdd: RDD[String]) = {
    //按“ ”分割
    val rdd = line_rdd.map(_.split(",")
      //转成Double类型
      .map(_.toDouble))
      //转成Vector格式
      .map(line => Vectors.dense(line))
    //读入行矩阵
    val rm = new RowMatrix(rdd)
    //打印列数
    println(rm.numRows())
    //打印行数
    println(rm.numCols())
    //    println(rm.rows.foreach(println))
  }

  def indexedRowMatrixTest(line_rdd: RDD[String]) = {
    //按“ ”分割
    val rdd = line_rdd.map(_.split(",")
      //转成Double类型
      .map(_.toDouble))
      //转成Vector格式
      .map(line => Vectors.dense(line))
      // 转化格式
      .map((vd) => new IndexedRow(vd.size, vd))

    //建立索引行矩阵实例
    val irm = new IndexedRowMatrix(rdd)
    //打印类型
    println(irm.getClass)
    //打印内容数据
    println(irm.rows.foreach(println))
//    irm.toRowMatrix() // 转化成单纯的行矩阵
//    irm.toCoordinateMatrix() // 转化成坐标矩阵
//    irm.toBlockMatrix() // 转化成块矩阵
  }

  def coordinateRowMatrixTest(line_rdd: RDD[String]) = {
    val rdd = line_rdd.map(_.split(",").map(_.toDouble))
      .map(vue => (vue(0).toLong, vue(1).toLong, vue(2)))
      //转化成坐标矩阵格式
      .map(v => new MatrixEntry(v._1, v._2, v._3))

    //实例化坐标矩阵
    val crm = new CoordinateMatrix(rdd)
    println(crm.entries.foreach(println))

  }





}
