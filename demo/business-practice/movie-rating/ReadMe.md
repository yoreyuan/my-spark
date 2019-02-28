

### 数据集准备
数据使用[我们使用的是MovieLens 100k数据集](https://link.jianshu.com/?t=http://files.grouplens.org/datasets/movielens/ml-100k.zip),
它包含了100000条用户对电影的点评（1分到5分）  

user.data  
用户id | 年龄 | 性别 | 职业 | 邮编  

ratings.data  
用户ID    电影ID    评分(1-5)     时间戳  

movies.data  
电影ID | 电影名称 | 上映日期 | 电影分类 等等   
电影ID | 电影名称 | 电影类型

### DAGScheduler
负责高层调度，将Application划分为不同的Stage，并生成有向无环图。

### TaskScheduler
负责具体的Stage内部的底层调调度，

### Stage  
Stage是用来计算中间结果的TaskSets。TaskSet中的Task的逻辑对于同一个RDD内的不同Partition都一样。
Stage在Shuffle的地方产生，此时下一个Stage要用到上一个Stage的全部数据，所以要等到上一个Stage全部执行完毕后才开始。

Stage有两种：
* ShuffleMapStage  
中间的所有Stage基本上都是这个类型的Stage。会产生中间的结果，并且是以文件的形式保存在集群里，
当job重用了同一个RDD，Stage经常被不同的Job共享。

* ResultStage  
一般是最后一个Stage

### Task
任务执行的基本单位，每个Task对应RDD的一个Partiton，每个task发送到一个节点上处理。

### RDD
不可变的、Lazy级别、粗粒度的数据集。包含一个或者多个分片（即Partition）


  
DataFrame
--
DataFrameAPI是从Spark 1.3开始就有的，它是一种以RDD为基础的**分布式无类型数据集**  
DataFrame类似于传统数据库中的二维表格。  

DataFrame和RDD的主要区别：  
前者带有schema元信息，即DataFrame表示的二维表数据集的每一列都带有名称和类型，这使得SparkSQL可以解析具体数据的机构信息，
从而对DataFrame中的数据源以对DataFrame的操作进行非常有效的优化，从而大幅提升了运行效率。


DataSet
--
DataSetAPI是从1.6版本提出的，在Spark 2.2的时候DataSet和DataFrame趋于稳定。  
与DataFrame不同的是，DataSet是强类型的，而DataFrame实际上就是DataSet\[Row](也就是Java的DataSet<Row>)  

DataSet是Lazy级别的，转换级别的算子作用于DataSet会得到一个新的DataSet。  
当执行算子被调用时，Spark的查询优化器会优化转换算子形成的逻辑计划，并生成一个物理计划，
该物理计划可以通过并行和分布式的方式执行。  

反观RDD，由于无从得知其内部的数据元素的具体内部结构，所以很难被Spark本身自行优化，  



