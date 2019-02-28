package yore.redis.test;

import redis.clients.jedis.Jedis;
import yore.redis.bean.RedisBean;
import yore.redis.service.RedisService;
import yore.redis.service.RedisServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yore on 2019/2/19 13:38
 */
public class MyApp {

    private RedisService redisService = new RedisServiceImpl();

    public static void main(String[] args) {

        String keyType = "RedisTestQUALINFO";
        Object redisTestResult = new String("Spark and Redis");

        /**
         * 根据业务需求，将Spark中提取到转换后的每条记录存入业务数据结构RedisBean的key和value；
         * 然后加入到RedisBeanList列表中。
         *   RedisBeanList是List<RedisBean>类型
         */
        List<RedisBean> redisBeanList = new ArrayList<RedisBean>();
        if("RedisTestQUALINFO".equals(keyType)){
            RedisBean redisBean = new RedisBean();
            redisBean.setKey("RedisTestQUALINFO");
            redisBean.setValue(redisTestResult.toString());
            redisBeanList.add(redisBean);
        }


        /**
         * 调用业务方法addToRedis，通过RedisServiceImpl.getSingle()获取Redis实例，
         * 然后遍历List<RedisBean>的每一个元素，调用 redis.lpush方法分别将Key值、value值lpush到Redis中。
         *
         * lpush完毕后，通过调用redis.close关闭连接。
         */
        addToRedis(redisBeanList);

    }

    public static void addToRedis(List<RedisBean> redisBeanList){
        Jedis redis = RedisServiceImpl.getSingle();
        for(RedisBean redisData : redisBeanList){
            redis.lpush(redisData.getKey(), redisData.getValue());
        }
        redis.close();
    }

}
