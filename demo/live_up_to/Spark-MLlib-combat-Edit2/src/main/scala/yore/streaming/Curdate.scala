/**
*   SQL 中自定义函数
*
*   Spark中支持的自定义函数分为三种
*     ① UDF (User Defined Function)
*     ② UDAF (User Defined Aggregation Function)
*     ③ UDTF (User Defined Table-Generating Function)
*
*/

package yore.streaming

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object MyUDF {

  /**
    * 日期加减
    *   tableEnv.registerFunction("adddate", new AddDate())
    *
    * <pre>
    *   adddate(date,INTERVAL expr unit)
    *   理论上unit 支持的类型有：
    *     microsecond ,second ,minute,hour ,day ,week ,month ,quarter ,year ,second_microsecond ,minute_microsecond ,
    *     minute_second ,hour_microsecond ,hour_second ,hour_minute ,day_microsecond ,day_second ,day_minute ,day_hour ,year_month
    *
    *   目前支持的有
    *     second ,minute,hour ,day ,week ,month
    * </pre>
    */
  // 未完全支持：adddate(date,INTERVAL expr unit)
  def addDateEval(date: Date, days: Object): Date = {
    val calendar = Calendar.getInstance
    println("adddate 1 **\t" + days)

    if(days.isInstanceOf[Int]){
      val d: Int = days.toString.trim.toInt
      calendar.add(Calendar.DAY_OF_YEAR, d)
      println("adddate 2  **\t" + new Date(calendar.getTimeInMillis))
      return new Date(calendar.getTimeInMillis)
    }

    if(days.isInstanceOf[String]){
      for(indicator_sql.intervalUnitRegex(interval,expr, unit) <- indicator_sql.intervalUnitRegex.findAllIn(days.toString)){
        val exprInt: Int = expr.trim.toInt
        calendar.setTime(date)
        return unit.trim match {
          case "month" => {
            calendar.add(Calendar.MONTH, exprInt)
            new Date(calendar.getTimeInMillis)
          }
          case "week" => {
            calendar.add(Calendar.WEEK_OF_YEAR, exprInt)
            new Date(calendar.getTimeInMillis)
          }
          case "day" => {
            calendar.add(Calendar.DAY_OF_YEAR, exprInt)
            new Date(calendar.getTimeInMillis)
          }
          case "hour" => {
            new Date(date.getTime + exprInt*1000*60*60)
          }
          case "minute" => {
            new Date(date.getTime + exprInt*1000*60)
          }
          case "second" => {
            new Date(date.getTime + exprInt*1000)
          }
        }
      }
    }
    null
  }


  /**
    * 当前日期
    *   tableEnv.registerFunction("curdate", new Curdate())
    *
    */
  def curdateEval(): Date = {
    new Date(System.currentTimeMillis())
  }


  /**
    * 日期间隔
    *   tableEnv.registerFunction("datediff", new DateDiff())
    *
    */
  def dateDiffEval(d1: Date,d2: Date): Int = {
    println("datediff**\t" + d1 +"\t" + d2)
    //    ((d1.getTime - d2.getTime)/1000/60/60/24).toInt
    92
  }


  /**
    * 给定日期当月的最后一天
    *   tableEnv.registerFunction("last_day", new LastDay())
    *
    */
  def lastDayEval(d1: Date): Date = {
    val calendar = Calendar.getInstance
    calendar.setTime(d1)
    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    new Date(calendar.getTimeInMillis)
  }


  /**
    * 日期格式化
    *   tableEnv.registerFunction("my_date_format", new DataFormat())
    *
    */
  def DataFormatEval(d1: Date, formatStr: String): String = {
    val sf = new SimpleDateFormat(formatStr)
    println(d1)
    sf.format(d1)
  }



}



