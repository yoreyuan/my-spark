Spark MLlib 机器学习
---

# 第一章：星星之火

## 1.2 大数据分析时代
一般来说，大数据分析需要设计以下5个方面，
* 有效的数据质量
* 优秀的分析引擎
* 合适的分析算法
* 对未来的合理预测
* 数据结果的可视化

## 1.3 简单、优雅、有效--这就是 Spark

## 1.4 核心 -- MLlib
目前 MLlib 中已经有通用的学习算法和工具类，包括统计、分类、回归、聚类、降维等。
- 分类回归
    * 线性模型（线性支持向量机SVM）
    * 朴素贝叶斯
    * 决策树
    * RF&GBDT
- 聚类
    * K-means
    * LDA
- 降维
    * SVD
    * PCA
- 特征抽取
    * TF-IDF
    * StandardScaler
    * Word2Vec
    * Normalizer
    * ChiSqSelector

- 推荐
    + ALS
- 关联规则
    + Fp-growth
- 优化
    + 随机梯度下降
    + L-BFGS
- 统计
    + 相关性
    + 分层抽样
    + 将设检验
- 算法评测
    + AUC
    + 准确率
    + 召回率
    + F-measuer

## 1.5 星星之火，可以燎原

---


# 第二章：Spark安装和开发环境配置

## 2.1 Windows单机模式 Spark 安装和配置

## 2.2 经典的WordCount

---

# 第三章： RDD详解

## 3.1 RDD是什么

## 3.2 RDD工作原理

## 3.3 RDD应用API详解

---

# 第四章：MLlib基本概念

## 4.1 MLlib基本数据类型
RDD 是 MLlib 是专用的数据格式，它参考了 Scala 函数式编程思想，并大胆引入统计分析概念，
**将存储数据转化成向量和矩阵的形式进行存储和计算**

### 4.1.1 多种数据类型
MLlib基本数据类型  
 
类型名称 | 释义
------- | ----
Local vector        |   本地向量集。主要向Spark提供一组可进行操作的数据集合
Labeled point       |   向量标签。让用户能够分类不同的数据集合
Local matrix        |   本地矩阵。将数据集合以矩阵形式存储在本地计算机中
Distributed matrix  |   分布式矩阵。将数据集合以矩阵形式存储和分布式计算机中

### 4.1.2 从本地向量集起步
MLlib使用本地化存储类型是向量，这里的向量主要由两类构成：
**稀疏型数据集(spares)** 和 **秘籍型数据集(dense)**

* 密集型数据集
[21  Vectors.dense()](src/main/scala/yore/spark/test_vector.scala)

* 稀疏型数据集.      (向量大小:要求大于等于输入数值的个数，向量索引：增加，与索引长度相同)
[31  Vectors.sparse()](src/main/scala/yore/spark/test_vector.scala)


### 4.1.3 向量标签的使用
[40 LabeledPoint() ](src/main/scala/yore/spark/test_vector.scala)

从文件中读取数据获取固定格式的数据集，作为矩阵  
[66  MLUtils.loadLibSVMFile()](src/main/scala/yore/spark/test_vector.scala)

### 4.1.4 本地矩阵的使用
[83  Matrices.dense()](src/main/scala/yore/spark/test_vector.scala)

### 4.1.5 分布式矩阵的使用
采用分布式矩阵进行存储的情况都是数据量非常大的，

1. 行矩阵  
[108  new RowMatrix(rdd)](src/main/scala/yore/spark/test_vector.scala)

2. 带有行索引的行矩阵  
[111  new IndexedRowMatrix(rdd)](src/main/scala/yore/spark/test_vector.scala)

3. 坐标矩阵和块矩阵  
[114  new CoordinateMatrix(rdd) ](src/main/scala/yore/spark/test_vector.scala)

4. 



## 4.2 MLlib数据统计基本概念

### 4.2.1 基本统计量
梳理统计中，基本统计量暴多数据的平均值、方差。

统计量的计算主要用到`Statistics`类库，

* 表 MLlib基本统计量介绍  

类型名称 | 释义
:---- | :---- 
colStats    | 以列为基础计算统计量的基本数据
chiSqTest   | 对数据集的数据进行皮尔逊距离计算，根据参数量的不同，返回值格式有差异
corr        | 对两个数据集进行相关系数计算，根据参量的不同，返回值格式有差异




















































