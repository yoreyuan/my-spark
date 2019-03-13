package yore.straming

import java.util.Properties

/**
  * Properties的工具类
  *
  * Created by yore on 2018-06-29 14:05
  */
object PropertiesUtil {

  /**
   *
   * 获取配置文件Properties对象
    *
   * @author yore
   * @return java.util.Properties
   * date 2018/6/29 14:24
   */
  def getProperties() :Properties = {

    val properties = new Properties()
    //读取源码中resource文件夹下的my.properties配置文件
    val reader = getClass.getResourceAsStream("/my.properties")
    properties.load(reader)
    properties
  }

  /**
    *
    * 获取配置文件中key对应的字符串值
    *
    * @author yore
    * @return java.util.Properties
    * date 2018/6/29 14:24
    */
  def getPropString(key : String) : String = {
    getProperties().getProperty(key)
  }

  /**
    *
    * 获取配置文件中key对应的整数值
    *
    * @author yore
    * @return java.util.Properties
    * date 2018/6/29 14:24
    */
  def getPropInt(key : String) : Int = {
    getProperties().getProperty(key).toInt
  }

  /**
    *
    * 获取配置文件中key对应的布尔值
    *
    * @author yore
    * @return java.util.Properties
    * date 2018/6/29 14:24
    */
  def getPropBoolean(key : String) : Boolean = {
    getProperties().getProperty(key).toBoolean
  }

}
