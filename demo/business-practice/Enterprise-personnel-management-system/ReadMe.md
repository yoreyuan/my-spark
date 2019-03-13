
DataSet开发实战企业人员管理系统应用案例
--
Spark 2.0.0新版本的主要更新包括API可用性、SQL 2003支持、性能改进、结构化流处理、R语言UDF自定义函数的支持、操作改进等。  
  
结构滑溜Structured Streaming是基于Spark SQL和Catalyst优化器构建的高级流API。
结构化流允许用户使用静态数据源相同的DataFrame/DataSet API对结构流数据进行编程，利用Catalyst优化器自动实现增量化查询计划。  

### 13.1 企业人员管理系统应用案例业务需求分析  

### 13.2 企业人员管理系统应用案例数据建模
数据：  
```
people.json
-------
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}

- name: 用户名
- name-value: 用户姓名的值
- age: 用户年龄
- age-value: 用户年龄的值

==
peopleScores.json
{"n": "Michael", "score":88}
{"n": "Andy", "score":100}
{"n": "Justin", "score":89}

```

### 13.3 通过SparkSession框架案例开发实战上下文环境
使用Spark 2.0的DataSet实战开发，背后会被Tungsten优化

* as[U]
- U 当是一个类时，类的字段将被映射到同名的列。大小写敏感度由spark.sql。caseSensitive配置决定。  
- U 当是一个元组时，列按照序号映射，例如：第一列将被分配到_1  
- U 当时一个原始类型，那么第一列将被DataFrame使用  

如果数据集的结构与所需的U类型不匹配，可以使用select与alias或as重新排列或重命名。  

Encoder:
```
/**
 * :: 实验性的 ::
 * 用于将一个JVM对象类型T使用Spark SQL标识
 *
 * == Scala ==
 * 通过sparkSession隐式转换，编码器可以自动创建，或者也可以调用静态方法[[Encoders]]显示创建.
 *
 * {{{
 *   import spark.implicits._
 *
 *   val ds = Seq(1, 2, 3).toDS() // implicitly provided (spark.implicits.newIntEncoder)
 * }}}
 *
 * == Java ==
 * 编码器调用静态方法[[Encoders]].
 *
 * {{{
 *   List<String> data = Arrays.asList("abc", "abc", "xyz");
 *   Dataset<String> ds = context.createDataset(data, Encoders.STRING());
 * }}}
 *
 * 编码器可以使用元组:
 *
 * {{{
 *   Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
 *   List<Tuple2<Integer, String>> data2 = Arrays.asList(new scala.Tuple2(1, "a");
 *   Dataset<Tuple2<Integer, String>> ds2 = context.createDataset(data2, encoder2);
 * }}}
 *
 * 或者采用Java Beans来构建编码格式:
 *
 * {{{
 *   Encoders.bean(MyClass.class);
 * }}}
 *
 * == 实现 ==
 *  - 编码器不需要线程安全的，一次不需要使用锁来维护。针对并发访问，编码器重用内部缓冲区提高性能
 *
 * @since 1.6.0
 */
```


### 13.4 通过map、flatMap、mapPartitions等分析企业人员管理系统
企业人员管理系统应用案例中企业人员信息包括姓名、年龄数据信息。
需要对企业人员的年龄进行统计分析，如估算员工10年后的工资数值等，涉及对员工的年龄进行计算。


### 13.5 通过dropDuplicate、coalesce、repartition等分析企业人员管理系统
* coalesce算子： 返回一个新的数据集，这个新的数据集有numpartitions个分区，在RDD中也有相似的coalesce定义。
coalesce算子操作的结果应用于窄依赖中，将1000个分区表转换成100个分区**多对一**，这个而过程不会发生shuffle，
相反，如果10个分区转换成100个分区**一对多**，将会发生shuffle。


### 13.6 通过sort、join、joinWith等分析企业人员管理系统
join返回的是关联后的表
joinWith关联后返回的每行是个元组


### 13.7 通过randomSplit、sample、select等分析企业人员管理系统

### 13.8 通过groupBy、agg、col等分析企业人员管理系统
在agg时使用常用算子时一定导入：**import org.apache.spark.sql.functions._**包

### 13.9 通过collect_list、collect_set等分析企业人员管理系统
collect_set是去重以后的姓名集合。结果中无重复元素
collect_list函数结果中包含重复元素；


### 13.10 通过avg、sum、countDistinct等分析企业人员管理系统

函数名 | 释义 
 ---  | ----  
* sum($"age")            |   年龄求和  
* avg($"age")            |   平均年龄  
* max($"age")            |   最大年龄
* min($"age")            |   最小年龄
* count($"age")          |   剑灵个数
* countDistinct($"age")  |   唯一年龄计数
* mean($"age")           |   平均年龄
* current_date()         |   当前时间
  

   
   
   
   
   