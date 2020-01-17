package yore

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  *
  * Created by yore on 2019/9/18 18:13
  */
object HBaseToDataFrame {
  val scc = new SparkConfClass()

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val sc = scc.getSc

    conf.set("hbase.zookeeper.quorum", "cdh6")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "demo")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = hbaseRDD.count()

    val files = Array("comm", "dname", "empno", "ename", "hiredate", "job", "loc", "mgr", "sal")
    case class Indicator(comm: String, dname: String, empno: String, ename: String, hiredate: String, job: String, loc: String, mgr: String, sal: String)

    val schemaForUsers = StructType(
      files.map(column => StructField(column, StringType, true))
    )

//    hbaseRDD.foreachPartition(p => {
//      while (p.hasNext){
//        for(kv <- p.next()._2.rawCells()){
//
//          val value = Bytes.toString(CellUtil.cloneValue(kv))
//          val qualifier = Bytes.toString(CellUtil.cloneQualifier(kv))
//
//
//          println(qualifier + " = " + value)
//        }
//      }
//    })

    hbaseRDD.map(f => {


    })


  }



}
