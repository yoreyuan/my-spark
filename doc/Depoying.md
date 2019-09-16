Deploying
==========

# 1 [Cluster Mode Overview](http://spark.apache.org/docs/latest/cluster-overview.html)

# 2 [Submitting Applications](http://spark.apache.org/docs/latest/submitting-applications.html)

# 3 [Spark Standalone Mode](http://spark.apache.org/docs/latest/spark-standalone.html)

## 3.1 安装
### 3.1.1 下载
```bash
 wget https://www-eu.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz 
```

### 3.1.2 解压
```bash
tar -zxf spark-2.4.3-bin-hadoop2.7.tgz
```

### 3.1.3 配置环境变量
```bash
vim  ~/.bash_profile
```
添加如下配置，保存退出
```bash
export SPARK_HOME=/opt/spark-2.4.3-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

### 3.1.4 配置文件`spark-defaults.conf`
```bash
cd $SPARK_HOME/conf
cp spark-defaults.conf.template spark-defaults.conf
vim spark-defaults.conf
```
修改如下配置，保存退出
```bash

spark.master                     spark://cdh6:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://cdh6:8020/spark/eventLog
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              1g
#spark.executor.extraJavaOptions  -XX:+PrintGCDetails -Dkey=value -Dnumbers="one two three"   

spark.yarn.jars                 	hdfs:///home/spark_lib/*
spark.yarn.dist.files				hdfs:///home/spark_conf/hive-site.xml
spark.sql.broadcastTimeout  		500

spark.history.fs.logDirectory		hdfs://cdh6:8020/spark/historyEventLog
spark.history.ui.port				18081
spark.history.fs.update.interval	10s
#	The number of application UIs to retain. If this cap is exceeded, then the oldest applications will be removed.
spark.history.retainedApplications	50
spark.history.fs.cleaner.enabled	false
spark.history.fs.cleaner.interval	1d
spark.history.fs.cleaner.maxAge		7d
spark.history.ui.acls.enable		false
```

### 3.1.5 修改配置文件`spark-env.sh`
```bash
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
vim spark-env.sh
```
添加如下配置，保存退出
```bash
export JAVA_HOME=/usr/local/zulu8
export SPARK_HOME=/opt/spark-2.4.3-bin-hadoop2.7
export SPARK_MASTER_IP=cdh6
export SPARK_EXECUTOR_MEMORY=1G
HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
SPARK_MASTER_HOST=cdh6
SPARK_MASTER_PORT=7077
# master SParkUI用的端口，如果不绑定，默认为8080，如果这个端口被占用，就会依次往后的端口绑定
SPARK_MASTER_WEBUI_PORT=8082
SPARK_LOCAL_IP=cdh6
# 默认是在tmp，但如果tmp被清除，会导致集群无法关停，所以最好配置上pid的路径
SPARK_PID_DIR=$SPARK_HOME/pids
```

### 3.1.6 slaves
```bash
cd $SPARK_HOME/conf
cp slaves.template slaves
vim slaves
```
添加如下配置，保存退出
```bash
cdh1
cdh2
cdh3
```

### 3.1.7 worker
将配置好的包发送到各Worker节点，并配置各Worker节点的环境变量

### 3.1.8 创建配置文件中用到的文件路径，并准备资源文件
```bash
hadoop fs -mkdir -p /home/spark_lib
hadoop fs -mkdir /home/spark_conf
hadoop fs -mkdir -p /spark/eventLog
hadoop fs -mkdir /spark/historyEventLog
hadoop fs -put $SPARK_HOME/jars/*  hdfs:///home/spark_lib/
hdfs dfs -put $HIVE_HOME/conf/hive-site.xml hdfs:///home/spark_conf/
cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/
mkdir $SPARK_HOME/pids
```

### 3.1.8 启动Spark
```bash
$SPARK_HOME/sbin/start-all.sh
$SPARK_HOME/sbin/start-history-server.sh
```

### 3.1.9 验证
在浏览器中查看spark的ui界面 
* master： [ http://cdh6:8082/ ](http://cdh6:8082/)
* worker:  [ http://cdh6:8083/ ](http://cdh6:8083/)
* History: [ http://cdh6:18081/ ](http://cdh6:18081/)


### 3.1.10 关闭
```bash
$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/stop-history-server.sh
```

# 4 [Running Spark on Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html)

# 5 [Running Spark on YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)

# 6 [Running Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)


***********
# 和Hive集成
将Hive的配置文件软连到Spark的classpath中（比如Spark的配置文件下）
```bash
ln -s $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/

```

然后启动`$SPARK_HOME/bin/spark-sql`时打印的日志可以看到如下日志，说明Spark和Hive是联通的，否则Spark会单独创建一个临时的仓库：
```
INFO  SharedState:54 - loading hive config file: file:/opt/spark-2.3.2-bin-hadoop2.7/conf/hive-site.xml
INFO  SharedState:54 - spark.sql.warehouse.dir is not set, but hive.metastore.warehouse.dir is set. Setting spark.sql.warehouse.dir to the value of hive.metastore.warehouse.dir ('/user/hive/warehouse').
INFO  SharedState:54 - Warehouse path is '/user/hive/warehouse'.
```

# 和Carbon Data集成
可以参考我的这篇blog [CarbonData安装和使用](https://blog.csdn.net/github_39577257/article/details/100130704)


