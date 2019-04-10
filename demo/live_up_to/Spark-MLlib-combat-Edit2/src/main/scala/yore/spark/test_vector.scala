package yore.spark

import org.apache.spark.mllib.linalg.{Vectors,Vector}

/**
  * 稀疏型数据集 -- spares
  * 密集型数据集 -- dense
  *
  * Created by yore on 2019/3/25 17:38
  */
object test_vector {

  def main(args: Array[String]): Unit = {
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

    /**
      *
      *
      */








  }

}
