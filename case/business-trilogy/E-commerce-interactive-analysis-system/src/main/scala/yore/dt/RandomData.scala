package yore.dt

/**
  *
  * Created by yore on 2019/2/26 14:16
  */
object RandomData {

  def main(args: Array[String]): Unit = {

    for(i <- 101 to 200){
      val a =  getRandomInt(0, 100)
      val b =  getRandomDouble(0, 2000)


      // {"logID":"01","userID":0,"time":"2016-10-17 15:42:45","typed":1,"consumed":33.36}

      var logStr = """{"logID":"$1","userID":$2,"time":"2016-10-$3 $4:42:45","typed":0,"consumed":$5}"""
      logStr = logStr.replace("$1", i.toString)
          .replace("$2", getRandomInt(0,10).toString)
          .replace("$3", getRandomInt(1,31).toString)
          .replace("$4", getRandomInt(0,24).toString)
          .replace("$5", getRandomDouble(0,3000).toString)

      println(logStr)
    }



  }

  def getRandomInt(startNum : Int = 0, endNum : Int) : Int = {
    val n1 = startNum + scala.util.Random.nextInt(endNum)
    n1
  }

  def getRandomDouble(startNum : Int = 0, endNum : Int) : Double = {
    val n2 = startNum + scala.util.Random.nextDouble() * (endNum - startNum)
    n2.formatted("%.2f").toDouble
  }

}
