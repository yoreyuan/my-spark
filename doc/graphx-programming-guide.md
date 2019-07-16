GraphX Programming Guide
-

**参考文档：**  
[Apache Spark GraphX官方文档](http://spark.apache.org/docs/latest/graphx-programming-guide.html#examples)  
[ApacheCN Spark 2.2.0中文文档](http://spark.apachecn.org/#/)
[GraphX Programming Guide](http://spark.apachecn.org/#/docs/9)

**目录**
- 概述
- 入门
- 属性 Graph
    * 示例属性 Graph
- Graph 运算符
    * 运算符的汇总表
    * Property 运算符
    * Structural 运算符
    * Join 运算符
    * 领域聚合
        + 聚合消息 (aggregateMessages)
        + Map Reduce Triplets Transition Guide (Legacy)
        + 计算级别信息
        + 收集相邻点
    * Caching 和 Uncaching
- Pregel API
- Graph Builders
- Vertex 和 Edge RDDs
    * VertexRDDs
    * EdgeRDDs
- 优化表示
- Graph 算子
    * PageRank
    * 连接组件
    * Triangle 计数
- 示例  
  
  
![graphx_logo.png](http://spark.apache.org/docs/latest/img/graphx_logo.png)

----

# 概述
GraphX 是 Spark 中用于图形和图形并行计算的新组件。在高层次上，GraphX 通过引入一个新的
[Graph](http://spark.apache.org/docs/latest/graphx-programming-guide.html#property_graph)抽象来扩展 
Spark [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) ：
一种具有附加到每个顶点和边缘的属性的定向多重图形。为了支持图计算，GraphX 公开了一组基本运算符
（例如： [subgraph](http://spark.apache.org/docs/latest/graphx-programming-guide.html#structural_operators) ，
[joinVertices](http://spark.apache.org/docs/latest/graphx-programming-guide.html#join_operators) 和 
[aggregateMessages](http://spark.apache.org/docs/latest/graphx-programming-guide.html#aggregateMessages) ）以及 
[Pregel](http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel) API 的优化变体。
此外，GraphX 还包括越来越多的图形[算法](http://spark.apache.org/docs/latest/graphx-programming-guide.html#graph_algorithms)
和[构建器](http://spark.apache.org/docs/latest/graphx-programming-guide.html#graph_builders)，以简化图形分析任务。  


# 入门
首先需要将 Spark 和 GraphX 导入到项目中，如下所示：
```scala
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
```
如果您不使用 Spark shell，您还需要一个 SparkContext。要了解有关 Spark 入门的更多信息，
请参考[Spark快速入门指南](http://spark.apache.org/docs/latest/quick-start.html)。


# 属性 Graph
[属性 Graph](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph) 是一个定向多重图形，
用户定义的对象附加到每个顶点和边缘。定向多图是具有共享相同源和目标顶点的潜在多个平行边缘的有向图。
**支持平行边缘的能力**简化了在相同顶点之间可以有多个关系（例如： 同事和朋友）的建模场景。每个顶点都由唯一的64位长标识符（ VertexId ）键入。 
**GraphX 不对顶点标识符施加任何排序约束**。类似地，边缘具有对应的源和目标顶点标识符。

属性图是通过 vertex (VD)和 edge (ED) 类型进行参数化的。这些是分别与每个顶点和边缘相关联的对象的类型。

> 当它们是原始数据类型（例如： int ，double 等等）时，GraphX 优化顶点和边缘类型的表示，通过将其存储在专门的数组中来减少内存占用。

在某些情况下，可能希望在同一个图形中具有不同属性类型的顶点。这可以通过继承来实现。例如，将用户和产品建模为二分图，我们可能会执行以下操作：

```scala
class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty
// The graph might then have the type:
import org.apache.spark.graphx._
var graph: Graph[VertexProperty, String] = null
```

像 RDD 一样，**属性图是不可变的，分布式的和容错的**。通过生成具有所需更改的新图形来完成对图表的值或结构的更改。
请注意，原始图形的大部分（即，未受影响的结构，属性和索引）在新图表中重复使用，可降低此内在功能数据结构的成本。
使用一系列顶点分割启发式方法，在执行器之间划分图形。与 RDD 一样，在发生故障的情况下，可以在不同的机器上重新创建图形的每个分区。

逻辑上，属性图对应于一对编码每个顶点和边缘的属性的类型集合( RDD )。因此，图类包含访问图形顶点和边的成员：
```
class Graph[VD, ED] {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
}
```

`VertexRDD[VD]` 和 `EdgeRDD[ED]` 分别扩展了 `RDD[(VertexId, VD)]` 和` RDD[Edge[ED]]` 的优化版本。 
`VertexRDD[VD]` 和 `EdgeRDD[ED]` 都提供了围绕图计算和利用内部优化的附加功能。 
我们在[顶点和边缘 RDD](http://spark.apachecn.org/#/docs/9?id=vertex_and_edge_rdds) 部分更详细地
讨论了 [VertexRDD](http://spark.apachecn.org/#/api/scala/index.html?id=org.apache.spark.graphx.vertexrdd)和 
[EdgeRDD API](http://spark.apachecn.org/#/api/scala/index.html?id=org.apache.spark.graphx.edgerdd)，
但现在它们可以被认为是 RDD[(VertexId, VD)] 和 RDD[Edge[ED]] 的简单 RDD。


## 示例属性 Graph
假设我们要构建一个由 GraphX 项目中的各种协作者组成的属性图。顶点属性可能包含用户名和职业。
我们可以用描述协作者之间关系的字符串来注释边：  
![property_graph.png](http://spark.apachecn.org/docs/img/6953ce1edbabb581001c1b124db1d69d.jpg)

生成的图形将具有类型签名：
```
val userGraph: Graph[(String, String), String]
```

从原始文件， RDD 甚至合成生成器构建属性图有许多方法，这些在[graph builders](http://spark.apache.org/docs/latest/graphx-programming-guide.html#graph_builders)
的一节中有更详细的讨论 。最普遍的方法是使用 [Graph object.](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph$)。
例如，以下代码从 RDD 集合中构建一个图：
```scala
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

// Assume the SparkContext has already been constructed
val sc: SparkContext
// Create an RDD for the vertices
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
// Create an RDD for edges
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
// Define a default user in case there are relationship with missing user
val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser)
```

在上面的例子中，我们使用了 [Edge](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Edge) case 类。
边缘具有 srcId 和 dstId 对应于源和目标顶点标识符。此外， Edge 该类有一个 attr 存储边缘属性的成员。

我们可以分别使用 graph.vertices 和 graph.edges 成员将图形解构成相应的顶点和边缘视图。
```scala
import org.apache.spark.graphx._
val graph: Graph[(String, String), String] // Constructed from above
// Count all users which are postdocs
graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
// Count all the edges where src > dst
graph.edges.filter(e => e.srcId > e.dstId).count
```

> 
注意， graph.vertices 返回一个 VertexRDD[(String, String)] 扩展 RDD[(VertexId, (String, String))] ，
所以我们使用 scala case 表达式来解构元组。另一方面， graph.edges 返回一个 EdgeRDD 包含 Edge[String] 对象。
我们也可以使用 case 类型构造函数，如下所示：
>
```
graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
```

除了属性图的顶点和边缘视图之外， GraphX 还暴露了三元组视图。三元组视图逻辑上连接顶点和边缘属性，生成 RDD[EdgeTriplet[VD, ED]] 
包含 [EdgeTriplet](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.EdgeTriplet) 
该类的实例。此连接可以用以下SQL表达式表示：
```sql
SELECT src.id, dst.id, src.attr, e.attr, dst.attr
FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
ON e.srcId = src.Id AND e.dstId = dst.Id
```
或图形为：
![triplet.png](http://spark.apache.org/docs/latest/img/triplet.png)

EdgeTriplet 类通过分别添加包含源和目标属性的 srcAttr 和 dstAttr 成员来扩展 Edge 类。 我们可以使用图形的三元组视图来渲染描述用户之间关系的字符串集合。
```scala
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
val graph: Graph[(String, String), String] // Constructed from above
// Use the triplets view to create an RDD of facts.
val facts: RDD[String] =
  graph.triplets.map(triplet =>
    triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
facts.collect.foreach(println(_))
```

# Graph 运算符
正如 RDDs 有这样的基本操作 map， filter， 以及 reduceByKey，性能图表也有采取用户定义的函数基本运算符的集合，产生具有转化特性和结构的新图。
定义了优化实现的核心运算符，并定义了 Graph 表示为核心运算符组合的方便运算符 [GraphOps](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.GraphOps) 。
不过，由于 Scala 的含义，操作员 GraphOps 可自动作为成员使用 Graph 。例如，我们可以通过以下方法计算每个顶点的入度（定义 GraphOps ）：
```
val graph: Graph[(String, String), String]
// Use the implicit GraphOps.inDegrees operator
val inDegrees: VertexRDD[Int] = graph.inDegrees
```
区分核心图形操作的原因 GraphOps 是能够在将来支持不同的图形表示。每个图形表示必须提供核心操作的实现，
并重用许多有用的操作 [GraphOps](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.GraphOps)。


## 运算符的汇总表
以下是两个定义的功能的简要摘要，但为简单起见 Graph， GraphOps 它作为 Graph 的成员呈现。
请注意，已经简化了一些功能签名（例如，删除了默认参数和类型约束），并且已经删除了一些更高级的功能，因此请参阅 API 文档以获取正式的操作列表。
```
/** Summary of the functionality in the property graph */
class Graph[VD, ED] {
  // Information about the Graph ===================================================================
  val numEdges: Long
  val numVertices: Long
  val inDegrees: VertexRDD[Int]
  val outDegrees: VertexRDD[Int]
  val degrees: VertexRDD[Int]
  // Views of the graph as collections =============================================================
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]
  // Functions for caching graphs ==================================================================
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]
  def cache(): Graph[VD, ED]
  def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]
  // Change the partitioning heuristic  ============================================================
  def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED]
  // Transform vertex and edge attributes ==========================================================
  def mapVertices[VD2](map: (VertexId, VD) => VD2): Graph[VD2, ED]
  def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]
  def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2]
  def mapTriplets[ED2](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]
  def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2])
    : Graph[VD, ED2]
  // Modify the graph structure ====================================================================
  def reverse: Graph[VD, ED]
  def subgraph(
      epred: EdgeTriplet[VD,ED] => Boolean = (x => true),
      vpred: (VertexId, VD) => Boolean = ((v, d) => true))
    : Graph[VD, ED]
  def mask[VD2, ED2](other: Graph[VD2, ED2]): Graph[VD, ED]
  def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
  // Join RDDs with the graph ======================================================================
  def joinVertices[U](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD): Graph[VD, ED]
  def outerJoinVertices[U, VD2](other: RDD[(VertexId, U)])
      (mapFunc: (VertexId, VD, Option[U]) => VD2)
    : Graph[VD2, ED]
  // Aggregate information about adjacent triplets =================================================
  def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexId]]
  def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]]
  def aggregateMessages[Msg: ClassTag](
      sendMsg: EdgeContext[VD, ED, Msg] => Unit,
      mergeMsg: (Msg, Msg) => Msg,
      tripletFields: TripletFields = TripletFields.All)
    : VertexRDD[A]
  // Iterative graph-parallel computation ==========================================================
  def pregel[A](initialMsg: A, maxIterations: Int, activeDirection: EdgeDirection)(
      vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId,A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED]
  // Basic graph algorithms ========================================================================
  def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double]
  def connectedComponents(): Graph[VertexId, ED]
  def triangleCount(): Graph[Int, ED]
  def stronglyConnectedComponents(numIter: Int): Graph[VertexId, ED]
}
```

## Property 运算符
与 RDD map 运算符一样，属性图包含以下内容：
```
class Graph[VD, ED] {
  def mapVertices[VD2](map: (VertexId, VD) => VD2): Graph[VD2, ED]
  def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]
  def mapTriplets[ED2](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]
}
```
这些运算符中的每一个产生一个新的图形，其中顶点或边缘属性被用户定义的 map 函数修改。

>> 请注意，在每种情况下，图形结构都不受影响。这是这些运算符的一个关键特征，它允许生成的图形重用原始图形的结构索引。
以下代码段在逻辑上是等效的，但是第一个代码片段不保留结构索引，并且不会从GraphX系统优化中受益：
>>

```
val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr)) }
val newGraph = Graph(newVertices, graph.edges)
```
> 而是 mapVertices 用来保存索引：
```
val newGraph = graph.mapVertices((id, attr) => mapUdf(id, attr))
```

这些运算符通常用于初始化特定计算或项目的图形以避免不必要的属性。
例如，给出一个以度为顶点属性的图（我们稍后将描述如何构建这样一个图），我们为PageRank初始化它：
```
// Given a graph where the vertex property is the out degree
val inputGraph: Graph[Int, String] =
  graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
// Construct a graph where each edge contains the weight
// and each vertex is the initial PageRank
val outputGraph: Graph[Double, Double] =
  inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
```

## Structural 运算符
目前GraphX只支持一套简单的常用结构运算符，我们预计将来会增加更多。以下是基本结构运算符的列表。
```
class Graph[VD, ED] {
  def reverse: Graph[VD, ED]
  def subgraph(epred: EdgeTriplet[VD,ED] => Boolean,
               vpred: (VertexId, VD) => Boolean): Graph[VD, ED]
  def mask[VD2, ED2](other: Graph[VD2, ED2]): Graph[VD, ED]
  def groupEdges(merge: (ED, ED) => ED): Graph[VD,ED]
}
```
该 reverse 运算符将返回逆转的所有边缘方向上的新图。这在例如尝试计算逆PageRank时是有用的。
由于反向操作不会修改顶点或边缘属性或更改边缘数量，因此可以在没有数据移动或重复的情况下高效地实现。

在 subgraph 操作者需要的顶点和边缘的谓词，并返回包含只有满足谓词顶点的顶点的曲线图（评估为真），并且满足谓词边缘边缘并连接满足顶点谓词顶点。
所述 subgraph 操作员可在情况编号被用来限制图形以顶点和感兴趣的边缘或消除断开的链接。例如，在以下代码中，我们删除了断开的链接：

```
// Create an RDD for the vertices
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                       (4L, ("peter", "student"))))
// Create an RDD for edges
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                       Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
// Define a default user in case there are relationship with missing user
val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser)
// Notice that there is a user 0 (for which we have no information) connected to users
// 4 (peter) and 5 (franklin).
graph.triplets.map(
  triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
).collect.foreach(println(_))
// Remove missing vertices as well as the edges to connected to them
val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
// The valid subgraph will disconnect users 4 and 5 by removing user 0
validGraph.vertices.collect.foreach(println(_))
validGraph.triplets.map(
  triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
).collect.foreach(println(_))
```
> 注意在上面的例子中只提供了顶点谓词。 如果未提供顶点或边缘谓词，则 subgraph 运算符默认为 true。

在 mask 操作者通过返回包含该顶点和边，它们也在输入图形中发现的曲线构造一个子图。这可以与 subgraph 运算符一起使用， 
以便根据另一个相关图中的属性限制图形。例如，我们可以使用缺少顶点的图运行连接的组件，然后将答案限制为有效的子图。

```
// Run Connected Components
val ccGraph = graph.connectedComponents() // No longer contains missing field
// Remove missing vertices as well as the edges to connected to them
val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
// Restrict the answer to the valid subgraph
val validCCGraph = ccGraph.mask(validGraph)
```

**groupEdges** 操作符将多边形中的平行边（即，顶点对之间的重复边）合并。 在许多数值应用中，可以将平行边缘（它们的权重组合）合并成单个边缘，从而减小图形的大小。

## Join 运算符
## 领域聚合
### 聚合消息 (aggregateMessages)
### Map Reduce Triplets Transition Guide (Legacy)
### 计算级别信息
### 收集相邻点
## Caching 和 Uncaching

- Pregel API
- Graph Builders
- Vertex 和 Edge RDDs
    * VertexRDDs
    * EdgeRDDs
- 优化表示
- Graph 算子
    * PageRank
    * 连接组件
    * Triangle 计数
- 示例  






