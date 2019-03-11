package yore.straming

import java.util.Date

/**
  *
  * Created by yore on 2019/3/5 17:40
  */
class ParamsList extends Serializable{
  var params1 : String = _
  var params2 : String = _
  var params3 : String = _
  var params4 : String = _
  var params5 : String = _
  var params6 : String = _
  var params7 : String = _

  var params10_Long : Long = _
  // 根据传入的这个字段，进行匹配，批量插入不同的SQL参数，如：params_Type = adclickedInsert。插入广告点击次数表的相关参数。
  var params_Type : String = _
  var length : Int = _

}



class UserAdClicked extends Serializable {
  var timestamp: String = _
  var ip: String = _
  var userID: String = _
  var adID: String = _
  var province: String = _
  var city: String = _
  var clickedCount: Long = _


  override def toString = s"UserAdClicked[ timestamp=$timestamp, ip=$ip, userID=$userID, adID=$adID, " +
    s"province=$province, city=$city, clickedCount=$clickedCount]"
}



class AdClicked extends Serializable{
  var timestamp: String = _
  var adID: String = _
  var province: String = _
  var city: String = _
  var clickedCount: Long = _

  override def toString = s"AdClicked[timestamp=$timestamp, adID=$adID, province=$province, city=$city, clickedCount=$clickedCount]"

}



class AdProvincetopN extends Serializable{
  var timestamp: String = _
  var adID: String = _
  var province: String = _
  var clickedCount: Long = _

  override def toString = s"AdProvincetopN[ timestamp=$timestamp, adID=$adID, province=$province, clickedCount=$clickedCount]"

}



class AdTrendCountHistory extends Serializable{
  var clickedCount: Long = _
}



class AdTrendStat extends Serializable{
  var _date: String = _
  var _hour: String = _
  var _minute: String = _
  var adID: String = _
  var clickedCount: Long = _

  override def toString = s"AdTrendStat[ _date=$_date, _hour=$_hour, _minute=$_minute, adID=$adID, clickedCount=$clickedCount]"
}



// ####################

object abc {
  var params1 : Int = _

  def main(args: Array[String]): Unit = {


    println("a = " + params1 )


    for(i <- 0 until 10){
      println(i)
    }


  }
}
