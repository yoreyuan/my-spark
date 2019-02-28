Spark商业案例之电商交互式分析系统应用案例
---
电商交互式分析系统应用案例：在实际生产环境下，一般都是以JJ2EE + Hadoop + Spark + DB(Redis)的方式实现综合技术栈，
使用Spark进行电商用户行为分析时一般都是交互式的。  
这里分析电商用户的多维度的行为特征，如：①分析特定时间段访问人数的TopN、②特定时间段购买金额排名的TopN、③注册后一周内购买金额排名TopN
④注册后一周内访问次数排名TopN、等。但这类技术和业务场景同样适用于门户网站(如网易、新浪等)，
也同样适合于在线教育系统（如分析在线教育系统的学员的行为），当然也适用于SNS（Social Networking Services）社交网络系统。


### 14.1 纯粹通过DataSet进行电商交互式分析系统中特定时段访问次数TopN
数据在如下目录下：`demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources`
```
1、 用户信息数据 user.json
{"userID":0,"name":"spark0","registeredTime":"2016-10-11 18:06:25"}
{"userID":1,"name":"spark1","registeredTime":"2016-10-11 18:06:25"}
{"userID":2,"name":"spark2","registeredTime":"2016-09-26 18:06:25"}
{"userID":3,"name":"spark3","registeredTime":"2016-10-04 18:06:25"}
{"userID":4,"name":"spark4","registeredTime":"2016-10-05 18:06:25"}
{"userID":5,"name":"spark5","registeredTime":"2016-10-05 18:06:25"}
{"userID":6,"name":"spark6","registeredTime":"2016-11-08 11:09:12"}
{"userID":7,"name":"spark7","registeredTime":"2016-10-07 18:06:25"}
{"userID":8,"name":"spark8","registeredTime":"2016-10-05 18:06:25"}
{"userID":9,"name":"spark9","registeredTime":"2016-10-07 18:06:25"}

root
 |-- name: string (nullable = true)
 |-- registeredTime: string (nullable = true)
 |-- userID: long (nullable = true)


2、用户访问记录 log.json
- type: type=0标识用户访问电商网站；type=1 用户在电商网站购买商品
- consumed: 消费金额
{"logID":"00","userID":0,"time":"2016-10-04 15:42:45","typed":0,"consumed":0.0}
{"logID":"01","userID":0,"time":"2016-10-17 15:42:45","typed":1,"consumed":33.36}
{"logID":"02","userID":0,"time":"2016-10-18 15:42:45","typed":0,"consumed":0.0}
{"logID":"03","userID":0,"time":"2016-10-14 15:42:45","typed":0,"consumed":0.0}
{"logID":"04","userID":0,"time":"2016-11-09 08:45:33","typed":1,"consumed":664.35}
{"logID":"05","userID":0,"time":"2016-10-13 15:42:45","typed":0,"consumed":0.0}
{"logID":"06","userID":0,"time":"2016-10-03 15:42:45","typed":1,"consumed":606.34}
{"logID":"07","userID":0,"time":"2016-10-04 15:42:45","typed":1,"consumed":120.72}
{"logID":"08","userID":0,"time":"2016-09-24 15:42:45","typed":1,"consumed":264.96}
{"logID":"09","userID":0,"time":"2016-09-25 15:42:45","typed":0,"consumed":0.0}

root
 |-- consumed: double (nullable = true)
 |-- logID: string (nullable = true)
 |-- time: string (nullable = true)
 |-- typed: long (nullable = true)
 |-- userID: long (nullable = true)

```

### 14.2 纯粹通过DataSet分析特定时段购买金额Top10和访问次数增长Top10


### 14.3 纯粹通过DataSet进行电商交互式分析系统中各种类型TopN分析实战详解
统计特定时段购买金额最多的Top5、
访问次数增长最多的Top5用户、
购买金额增长最多的Top5用户、
注册之后，前两周内访问最多的Top10、
注册之后，前两周内购买总额最多的Top10


### 14.4 
Window时间窗口函数是在Spark 2.0.0 中新增的API函数，
根据Window函数中给定的时间戳指定列，生成滚动时间窗口。
时间窗口是左开右闭的，例如：
12：05 将落在窗口 [12:05, 12:10) ,不落在窗口[12:00, 12:05)   

    


functions.scala代码中的API函数清单如下：  
![表 functions.scala API 1](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_1.jpg)  
![表 functions.scala API 2](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_2.jpg)  
![表 functions.scala API 3](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_3.jpg)  
![表 functions.scala API 4](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_4.jpg)  
![表 functions.scala API 5](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_5.jpg)  
![表 functions.scala API 6](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_6.jpg)  
![表 functions.scala API 7](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_7.jpg)  
![表 functions.scala API 8](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_8.jpg)  
![表 functions.scala API 9](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_9.jpg)  
![表 functions.scala API 10](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_10.jpg)  
![表 functions.scala API 11](demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/IMG/fun_API_11.jpg)  

```scala
import org.apache.spark.sql.functions._

abs() // 计算绝对值
add_months()  // 返回开始日期之后为numMonths的日期
approx_count_distinct()   //聚合函数：返回组中不同记录中的相似数量
array_contains()    //如果数组中包含value，则返回true
array() // 创建一个新的数组列。输入列必须具有相同的数据类型


…

```








































