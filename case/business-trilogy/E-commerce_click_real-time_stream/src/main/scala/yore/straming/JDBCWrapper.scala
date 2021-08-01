package yore.straming

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.mutable.ListBuffer

/**
  *
  * Created by yore on 2019/3/5 17:34
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
    try {
      val conn = DriverManager.getConnection(
        PropertiesUtil.getPropString("mysql.db.url"),
        PropertiesUtil.getPropString("mysql.user"),
        PropertiesUtil.getPropString("mysql.password"));
      dbConnectionPool.put(conn)
    } catch {
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
    * @return
    */
  def doBatch(sqlText: String, paramsList: ListBuffer[ParamsList]): Array[Int] = {
    val conn: Connection = getConnection
    var ps: PreparedStatement = null
    var result: Array[Int] = null
    try {
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(sqlText)

      for (paramters <- paramsList) {
        paramters.params_Type match {
          case "adclickedInsert" => {
            println("adclickedInsert")
            ps.setObject(1, paramters.params1)
            ps.setObject(2, paramters.params2)
            ps.setObject(3, paramters.params3)
            ps.setObject(4, paramters.params4)
            ps.setObject(5, paramters.params5)
            ps.setObject(6, paramters.params6)
            ps.setObject(7, paramters.params10_Long)
          }
          case "blacklisttableInsert" => {
            println("blacklisttableInsert")
            ps.setObject(1, paramters.params1)
          }
          case "adclickedcountInsert" => {
            println("adclickedcountInsert")
            ps.setObject(1, paramters.params1)
            ps.setObject(2, paramters.params2)
            ps.setObject(3, paramters.params3)
            ps.setObject(4, paramters.params4)
            ps.setObject(5, paramters.params10_Long)
          }
          case "adprovincetopnInsert" =>{
            println("adprovincetopnInsert")
            ps.setObject(1, paramters.params1)
            ps.setObject(2, paramters.params2)
            ps.setObject(3, paramters.params3)
            ps.setObject(4, paramters.params10_Long)
          }
          case "adclickedtrendInsert" => {
            println("adclickedtrendInsert")
            ps.setObject(1, paramters.params1)
            ps.setObject(2, paramters.params2)
            ps.setObject(3, paramters.params3)
            ps.setObject(4, paramters.params4)
            ps.setObject(5, paramters.params10_Long)
          }
          case "adclickedUpdate" => {
            println("adclickedUpdate")
            ps.setObject(1, paramters.params10_Long)
            ps.setObject(2, paramters.params1)
            ps.setObject(3, paramters.params2)
            ps.setObject(4, paramters.params3)
            ps.setObject(5, paramters.params4)
            ps.setObject(6, paramters.params5)
            ps.setObject(7, paramters.params6)
          }
          case "blacklisttableUpdate" => {
            println("blacklisttableUpdate")
            ps.setObject(1, paramters.params1)

          }
          case "adclickedcountUpdate" => {
            println("adclickedcountUpdate")
            ps.setObject(1, paramters.params10_Long)
            ps.setObject(2, paramters.params1)
            ps.setObject(3, paramters.params2)
            ps.setObject(4, paramters.params3)
            ps.setObject(5, paramters.params4)
          }
          case "adprovincetopnUpdate" => {
            println("adprovincetopnUpdate")
            ps.setObject(1, paramters.params10_Long)
            ps.setObject(2, paramters.params1)
            ps.setObject(3, paramters.params2)
            ps.setObject(4, paramters.params3)
          }
          case "adprovincetopnDelete" => {
            println("adprovincetopnDelete")
            ps.setObject(1, paramters.params1)
            ps.setObject(2, paramters.params2)
          }
          case "adclickedtrendUpdate" => {
            println("adclickedtrendUpdate")
            ps.setObject(1, paramters.params10_Long)
            ps.setObject(2, paramters.params1)
            ps.setObject(3, paramters.params2)
            ps.setObject(4, paramters.params3)
            ps.setObject(5, paramters.params4)
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


  /**
    * 批量查询
    *
    * @param sqlText
    * @param paramsList
    * @param callBack
    */
  def doQuery(sqlText: String, paramsList: Array[_], callBack: ResultSet => Unit){
    val conn: Connection = getConnection()
    var ps: PreparedStatement = null
    var result: ResultSet = null

    try{
      ps = conn.prepareStatement(sqlText)
      if(paramsList != null){
        for(i <- 0 until paramsList.length ){
          ps.setObject(i + 1, paramsList(i))
        }
      }
      result = ps.executeQuery()
      callBack(result)
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(ps != null){
        try{
          ps.close()
        }catch {
          case e: SQLException => e.printStackTrace()
        }
      }

      if(conn != null){
        try{
          dbConnectionPool.put(conn)
        }catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
    }
  }

}
