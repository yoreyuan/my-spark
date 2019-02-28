package yore.redis.service;

//import sun.security.krb5.Config;
//import com.sun.deploy.config.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by yore on 2019/2/19 09:30
 */
public class RedisServiceImpl implements RedisService{

    /**
     * Redis配置的一个单例
     */
    static class RedisConf {

        /**
         * 只适合单线程环境（不推荐）
         *
         * 多线程时，当两个线程同时调用getInstance()方法时，instance没有创建时，会同时创建两个对象
         */
        private static RedisConf instance = null;
        private RedisConf(){
        }
        public static RedisConf getInstance(){
            if(instance == null){
                instance = new RedisConf();
            }
            return instance;
        }

        /**
         * 获取配置文件映射map对象
         *
         * @date: 2019/2/19 10:32 AM
         */
        public Map<String, String> getRedisParams(){
            Properties prop = new Properties();
            try {
                prop.load(RedisConf.class.getResourceAsStream("/config.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }

            Map<String, String> map = new HashMap<String, String>(
                    (Map)prop
            );
            return map;
        }


    }

    private static final Map<String, String> REDIS_CONFIG = RedisConf.getInstance().getRedisParams();
    private static final String REDIS_IP = REDIS_CONFIG.get("redis.ip");
    private static final String REDIS_PORT = REDIS_CONFIG.get("redis.port");
    private static final String REDIS_PASSWORD = REDIS_CONFIG.get("redis.password");
    private static final int REDIS_TIMEOUT = 2000;

    private static JedisPool pool;

    /**
     * 从Redis访问池中获取redis实例
     *
     * @return Redis实例
     * @date: 2019/2/19 10:38 AM
     */
    public static Jedis getFromPool(){
        if(pool == null){
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(Integer.valueOf(REDIS_CONFIG.get("redis.pool.maxTotal")));
            config.setMaxIdle(Integer.valueOf(REDIS_CONFIG.get("redis.pool.maxIdle")));
            config.setMaxWaitMillis(Integer.valueOf(REDIS_CONFIG.get("redis.pool.maxWait")));
            config.setTestOnBorrow(Boolean.valueOf(REDIS_CONFIG.get("redis.pool.testOnBorrow")));
            config.setTestOnReturn(Boolean.valueOf(REDIS_CONFIG.get("redis.pool.testOnReturn")));
            pool = new JedisPool(config , REDIS_IP, Integer.valueOf(REDIS_PORT), REDIS_TIMEOUT, REDIS_PASSWORD);
        }
        return pool.getResource();
    }

    public static void returnResource(Jedis redis){
        pool.returnResource(redis);
    }

    /**
     * 获取redis实例
     *
     * @return redis实例
     * @date: 2019/2/19 11:32 AM
     */
    public static Jedis getSingle(){
        Jedis redis = new Jedis(REDIS_IP, Integer.valueOf(REDIS_PORT), REDIS_TIMEOUT);
        redis.auth(REDIS_PASSWORD);
        redis.select(10);
        return redis;
    }


}

