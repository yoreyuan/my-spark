package yore.sql

import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yore on 2019/1/21 16:39
  */
object DataSetCh {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataSet Transformation")
      .getOrCreate()

    val sc = spark.sparkContext

    // [{"name":"name","type":"string"},{"name":"favorite_color","type":["string","null"]},{"name":"favorite_numbers","type":{"type":"array","items":"int"}}]
    case class Users(name : String, favorite_color : String = null, favorite_numbers : Array[Int])
//    case class Person(name : String, age : Long)

    import spark.implicits._

    var usersDF = spark.read.parquet("case/business-trilogy/movie-rating/src/main/resources/users.parquet")

//    val usersDS = usersDF.as[Person]

//    usersDS.printSchema()

    /*usersDF.map(user =>{
      println(user.getAs[String](0) + "\t" + user.getAs[String](1) + "\t" + user.getAs[Array[Int]](2))
      user.getAs(0)
    }).take(3)*/

    usersDF.foreachPartition(row =>{
//      System.err.println(row.getAs[String](0))
//      println(row)
      row.map(r =>{
        System.err.println(r)
        Users(r.getAs[String](0), r.getAs[String](1), /*row.getAs[Array[Int]](2)*/null )
      }).foreach(println)
    })



//    usersDF.show()




  }

}
