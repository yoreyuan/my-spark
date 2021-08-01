package yore.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
  *
  * Created by yore on 2019/3/25 13:49
  */
object Guide_Demo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("E-commerce-interactive-analysis-system")
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    // Assume the SparkContext has already been constructed
    val sc: SparkContext = spark.sparkContext

    //
    /**
      * Create an RDD for the vertices
      *
      * Vertex Table
      *   Id      Property(V)
      *   3       (rxin, 学生)
      *   7       (jgozal, 博士后)
      *   5       (Franklin, 教师)
      *   2       (istoica, 教师)
      */
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //
    /**
      * Create an RDD for edges
      *
      * Edge Table
      *   SrcId   DstId   Property(E)
      *   3       7         合作者
      *   5       3         指导教授
      *   2       5         同事
      *   5       7         项目负责人
      */
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

//    val graph: Graph[(String, String), String] // Constructed from above
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))

  }

}
