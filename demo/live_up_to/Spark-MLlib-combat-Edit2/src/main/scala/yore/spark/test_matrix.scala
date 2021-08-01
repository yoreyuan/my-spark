package yore.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  *
  * Created by yore on 2019/3/25 17:38
  */
object test_matrix {

  def main0(args: Array[String]): Unit = {
    /*
     * 1.0  2.0
     * 3.0  4.0
     * 5.0  6.0
     */
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)

    /*
     * (1,0) 9.0
     * (2,1) 6.0
     * (1,1) 8.0
     */
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(1, 2, 1), Array(9, 6, 8))
    println(sm)
  }


  /*
   * 1 行矩阵
   */
  def main1(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("test_matrix")
    val sc = new SparkContext(conf)

    val rows: RDD[Vector] = sc.makeRDD(Seq(
      Vectors.dense(1,2 , 3),
      Vectors.dense(4, 5, 6)
    ))
    println(rows.foreach(row => println(row)))
    val mat: RowMatrix = new RowMatrix(rows)
    val m = mat.numRows();
    val n = mat.numCols();
    println(m + " X " + n)
  }


  /*
   * 2 索引行矩阵
  */
  def main2(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("test_matrix")
    val sc = new SparkContext(conf)

    val iv1 = IndexedRow(0, Vectors.dense(1, 2, 3))
    val iv2 = IndexedRow(1, Vectors.dense(4, 5, 6))
    val rows: RDD[IndexedRow] = sc.makeRDD(Seq(iv1, iv2))
    println(rows.foreach(row => println(row)))
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    val m = mat.numRows();
    val n = mat.numCols();
    println(m + " X " + n)
  }



  /*
   * 3 坐标矩阵
  */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("test_matrix")
    val sc = new SparkContext(conf)

    val e1 = MatrixEntry(0, 0, 0)
    val e2 = MatrixEntry(1, 1, 1)
    val e3 = MatrixEntry(2, 2, 4)
    val e4 = MatrixEntry(3, 3, 9)
    val entries: RDD[MatrixEntry] = sc.makeRDD(Seq(e1, e2, e3, e4))
    println(entries.foreach(row => println(row)))

    val mat: CoordinateMatrix = new CoordinateMatrix(entries)
    val m = mat.numRows();
    val n = mat.numCols();
    println(m + " X " + n)
  }


}
