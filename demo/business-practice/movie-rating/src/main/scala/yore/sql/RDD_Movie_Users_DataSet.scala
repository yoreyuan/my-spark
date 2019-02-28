package yore.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 公国DataSet实战电影点评系统案例
  *
  * DataSet可以由DataFrame转换过来，
  * myDataFrame.as[myClass],就可以得到封装了myClass类型的DataSet，
  *
  *
  *  注意，
  *     新版DataSet进行操作需要进行相应的encode操作
  *
  *  RDD的cache方法等于MEMORY_ONLY级别的persist
  *  DataSet级别的cache方法等于MEMORY_AND_DISK级别的persist，因为重新计算的代价很昂贵。
  *
  *
  * Created by yore on 2019/1/7 16:34
  */
object RDD_Movie_Users_DataSet {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]")
          .setAppName("RDD_Movie_Users_Analyzer")

        /**
        * Spark 2.0之后引入SparkSession，封装了SparkContext和SQLContext，
        * 并且会在builder的getOrCreate方法中判断是否有符合要求的SparkSession存在，有则使用，没有则创建。
        *
        */
        val spark = SparkSession.builder().config(conf).getOrCreate()
        /*val spark = SparkSession.builder().master("local")
          .appName("")
          .config("", "")
          .getOrCreate()*/



        //    val sc = SparkContext(conf)
        val sc = spark.sparkContext
        // 设置spark程序运行的日志级别，
        sc.setLogLevel("WARN")

        //引入一个隐式转换
//        import spark.implicits._
//        import org.apache.spark.sql.types._


        val usersRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/user.data")
        val moviesRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/movies.data")
        val ratingsRDD = sc.textFile("demo/business-practice/movie-rating/src/main/resources/ratings.data")




        case class User(UserID:String, Age:String, Gender:String, OccupationID:String, Zip_Code:String)
//        implicit val userEncoder = org.apache.spark.sql.Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING)
//        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row]
        case class Rating(UserID:String, MovieID:String, Rating:Double, Timestamp: String)

//        val usersDataSet = spark.createDataset[User](usersForDSRdd)
//        val usersDataSet = spark.read.textFile("demo/business-practice/movie-rating/src/main/resources/user.data")
//            .map(_.split("\\|"))
//            .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
//            .as[User]

//        val usersForDSRdd = usersRDD.toDS().map(line =>{
//            val ls = line.split("\\|")
//            User(ls(0).trim, ls(1).trim, ls(2).trim, ls(3).trim, ls(4).trim)
//        }).show(10)

//        val usersForDSRdd = usersRDD.map(_.split("\\|"))
//            .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
//            .toDS()
//            .as[User](Class[User])
//            .show(2)



        val usersForDSRdd = usersRDD.map(_.split("\\|"))
            .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim) )

//        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[User]
//        implicit val stringIntMapEncoder: Encoder[User] = ExpressionEncoder()
//        val usersDataSet = spark.createDataset(usersForDSRdd)

//        usersDataSet.show(10)


        // 直接创建DataSet
//        usersDataSet.show(10)


//
//    // DataFrame中间某一步转RDD操作
//    ratingsDataFrame.select("MovieID", "Rating")
//        .groupBy("MovieID")
//        .avg("Rating")
//        .rdd
//        .map(row => (row(1), (row(0), row(1))))
//        .sortBy(_._1.toString.toDouble, false)
//        .collect
//        .take(10)
//        .foreach(println)


        //-------------
//        val ratingsForDSRDD = ratingsRDD.map(_.split("\t"))
//            .map(line =>
//            Rating(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
//        val ratingsDataSet = spark.createDataset(ratingsForDSRDD)
//
//        import spark.sqlContext.implicits._
//        ratingsDataSet.filter(s"MovieID = 1193")
//            .join(usersDataSet, "UserID")
//            .select("Gender", "Age")
//            .groupBy("Gender", "Age")
//            .count
////            .orderBy($"Gender".desc, $"Age").show()




    // 最后关闭SparkSession
    spark.stop()

  }

}
