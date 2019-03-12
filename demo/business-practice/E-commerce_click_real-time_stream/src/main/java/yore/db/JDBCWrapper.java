package yore.db;

import java.sql.*;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yore on 2019/3/6 16:02
 */
public class JDBCWrapper {

    static final Integer POOL_SIZE = 10;

    /**
     * 连接池队列
     *
     * java.util.concurrent包下的新类，是一个阻塞的线程安全的队列，底层采用链表实现。
     * LinkedBlockingQueue构造的时候若没有指定大小，则默认大小为Integer.MAX_VALUE，当然也可以在构造函数的参数中指定大小。LinkedBlockingQueue不接受null。
     *
     * 添加元素：
     *      add 方法在添加元素的时候，若超出了队列的长度会直接抛出异常：
     *      put 方法，若向队尾添加元素的时候发现队列已经满了会发生阻塞一直等待空间，以加入元素。
     *      offer 方法在添加元素时，如果发现队列已满无法添加的话，会直接返回false。
     *
     * 从队列中取出并移除头元素：
     *      poll: 若队列为空，返回null。
     *      remove:若队列为空，抛出NoSuchElementException异常。
     *      take:若队列为空，发生阻塞，等待有元素。
     */
    private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<>(POOL_SIZE);

    private JDBCWrapper(){
        for(int i =0;i<POOL_SIZE; i++){
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
        try {
            return dbConnectionPool.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return dbConnectionPool.poll();
        }
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
