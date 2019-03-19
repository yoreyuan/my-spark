package yore.spark

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.mutable.ListBuffer

/**
  *
  * Created by yore on 2018/11/5 17:34
  */
object JDBCWrapper {
  private var jdbcInstance : JDBCWrapper = _
  def getInstance() : JDBCWrapper = {
    synchronized{
      if(jdbcInstance == null){
        jdbcInstance = new JDBCWrapper()
      }
    }
    jdbcInstance
  }

}


class JDBCWrapper {
  // 连接池的大小
  val POOL_SIZE : Int = PropertiesUtil.getPropInt("mysql.connection.pool.size")

  val dbConnectionPool = new LinkedBlockingDeque[Connection](POOL_SIZE)
  try
    Class.forName(PropertiesUtil.getPropString("mysql.jdbc.driver"))
  catch {
    case e: ClassNotFoundException => e.printStackTrace()
  }

  for(i <- 0 until POOL_SIZE){
    try{
      val conn = DriverManager.getConnection(
        PropertiesUtil.getPropString("mysql.db.url"),
        PropertiesUtil.getPropString("mysql.user"),
        PropertiesUtil.getPropString("mysql.password"));
      dbConnectionPool.put(conn)
    }catch {
      case e : Exception => e.printStackTrace()
    }
  }

  def getConnection(): Connection = synchronized{
    while (0 == dbConnectionPool.size()){
      try{
        Thread.sleep(20)
      }catch {
        case e : InterruptedException => e.printStackTrace()
      }
    }
    dbConnectionPool.poll()
  }


  /**
    * 批量插入
    *
    * @param sqlText    sql语句字符
    * @param paramsList 参数列表
    * @return Array[Int]
    */
  def doBatch(sqlText: String, paramsList: ListBuffer[ParamsList]): Array[Int] = {
    val conn: Connection = getConnection()
    var ps: PreparedStatement = null
    var result: Array[Int] = null

    try{
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(sqlText)

      for (paramters <- paramsList) {
        paramters.params_Type match {
          case "real_risk" => {
            println("$$$\treal_risk\t" + paramsList)
            // // p_num, risk_rank, mor_rate, ch_mor_rate, load_time
            ps.setObject(1, paramters.p_num)
            ps.setObject(2, paramters.risk_rank)
            ps.setObject(3, paramters.mor_rate)
            ps.setObject(4, paramters.ch_mor_rate)
            ps.setObject(5, paramters.load_time)
          }
        }
        ps.addBatch()
      }
      result = ps.executeBatch
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) try {
        ps.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }

      if (conn != null) try {
        dbConnectionPool.put(conn)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }
    result
  }


}
