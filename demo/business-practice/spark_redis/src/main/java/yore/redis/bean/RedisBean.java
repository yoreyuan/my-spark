package yore.redis.bean;

/**
 * Created by yore on 2019/2/19 11:34
 */
public class RedisBean {
    protected String key;
    protected String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
