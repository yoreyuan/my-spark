package yore

import org.apache.spark.sql.jdbc.JdbcDialect

/**
  * 可以查看 org.apache.spark.sql.jdbc.MySQLDialect
  *
  * Created by yore on 2020/3/20 00:02
  */
private case object HiveSqlDialect extends JdbcDialect{

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = {
    if(colName.contains(".")){
      val colArr = colName.split("\\.")
      colArr(0) + s".`${colArr(1)}`"
    }else{
      s"`$colName`"
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }


}
