[hortonworks-spark/shc](https://github.com/hortonworks-spark/shc)
----

# Apache Spark - Apache HBase Connector
借助它，用户可以在 DataFrame 和 DataSet 级别使用 Spark-SQL 操作 HBase。

## Datatype conversion
SHC支持三种内部Serdes：`Avro`，`Phoenix`和`PrimitiveType`。用户可以通过在目录中定义'tableCoder' 来指定要使用的serde。


## Creatable DataSource
该库支持对HBase进行读取和写入。

```bash
# 下载
git clone https://github.com/hortonworks-spark/shc.git

# 查看可用的tag或分支
git tag
git branch -r
# 切换对应的tag或分支
#git checkout origin/branch-2.3_HBase2
git checkout origin/branch-1.6

# 编译
mvn clean package -DskipTests

# 运行SHC示例
./bin/spark-submit --verbose --class org.apache.spark.sql.execution.datasources.hbase.examples.HBaseSource --master yarn-cluster \
--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 \
--repositories http://repo.hortonworks.com/content/groups/public/ \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml \
shc-examples-1.1.1-2.1-s_2.11-SNAPSHOT.jar




```











