package yore.db;

import java.sql.ResultSet;

/**
 *
 * Created by yore on 2019/3/6 14:42
 */
public interface ExecuteCallBack {
    void resultCallBack(ResultSet result ) throws Exception;
}
