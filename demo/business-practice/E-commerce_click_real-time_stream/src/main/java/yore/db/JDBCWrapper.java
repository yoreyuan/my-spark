package yore.db;

import java.sql.*;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yore on 2019/3/6 16:02
 */
public class JDBCWrapper {

    private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<>();

    private JDBCWrapper(){
        for(int i =0;i<10; i++){
            try {
                Connection conn = DriverManager.getConnection(
                        "jdbc:mysql://cdh1:3306/sparkstreaming",
                        "root",
                        "123456"
                );
                dbConnectionPool.put(conn);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    private static JDBCWrapper jdbcInstance = null;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    public static JDBCWrapper getJDBCInstance(){
        if(jdbcInstance == null){
            synchronized (JDBCWrapper.class){
                if(jdbcInstance == null){
                    jdbcInstance = new JDBCWrapper();
                }
            }
        }
        return jdbcInstance;
    }


    public synchronized Connection getConnection(){
        while (0 == dbConnectionPool.size()){
            try {
                Thread.sleep(20);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        return dbConnectionPool.poll();
    }

    /**
     * 批量插入
     *
     * @param sqlText sql语句字符
     * @param paramsList 参数列表
     * @return
     */
    public int[] doBatch(String sqlText, List<Object[]> paramsList){
        Connection conn = getConnection();
        PreparedStatement ps = null;
        int[] result = null;

        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(sqlText);

            for(Object[] paramters : paramsList){
                for(int i=0; i<paramters.length; i++){
                    ps.setObject(i + 1, paramters[i]);
                }
                ps.addBatch();
            }
            result = ps.executeBatch();
            conn.commit();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }


    public void doQuery(String sqlText, Object[] paramsList, ExecuteCallBack callBack){
        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet result = null;

        try {
            ps = conn.prepareStatement(sqlText);
            if(paramsList != null){
                for(int i =0 ; i< paramsList.length; i++){
                    ps.setObject(i + 1, paramsList[i]);
                }
            }
            result = ps.executeQuery();

            callBack.resultCallBack(result);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
