package yore

/**
  *
  * Created by yore on 2018/12/24 15:09
  */
object MySqrt extends App {

  // 1.4142135623730951
//  println(Math.sqrt(2))

//  println(mySqqrt(2, 16))
//print(Math.pow(10, -4))

  /**
    *  牛顿迭代法
    *
    * @param x
    */
  def mySqqrt(x : Double, precision : Int): Double = {
    var y = 1.0
    var flag = true
    var middle = 0.0

    while (flag){
      middle = (y + x/y)/2 - y
      if(Math.abs(middle) < Math.pow(10, -precision)){
        flag = false
      }else{
        y = (y + x/y)/2
      }

    }
    y
  }


//  val str = "Yore Yuan"
//  println(str.take(1))
//  println(str.drop(1))

//  printf(raw"\n is a new line")
//  val price = 11
//  printf(s"$$$price")

  /*import scala.io
  val name = StdIn.readLine("Yore name: ")
  print("Yore age: ")
  val age = StdIn.readInt()
  println(s"Hello, ${name}! Next year, you will be ${age +1}.")*/


  /**
    * 让变量遍历<-右侧的表达式的所有制。
    *
    */
  /*for(i <- 0 to 10){
    println(i)
  }*/
  /*val s = "Scala"
  for(i <- 0 to s.length - 1){
    println(s.apply(i))
  }*/
  /*for(ch <- "Scala"){
    println(ch)
  }*/

  /*import util.control.Breaks._
  breakable(
    for(ch <- "Scala"){
      if(ch.equals('a')){
        break()
      }
    }
  )*/

  /*for(i <- 1 to 3; j <- 4 to 6 if i != j ){
    print(f"${10 * i + j}%3d")
  }*/
  /*for(i <- 1 to 3; from = 4 - i; j <- from to 3){
    print(f"${10 * i + j}%3d")
  }*/
  /**
   * yield 会构造一个集合，每次迭代生成集合中的一个值
   * for推导式
   *
   */
  /*val vector = for(i <- 1 to 10) yield i % 3
  println(vector)*/

  // 变长参数
  /*def sum (args : Int*): Unit ={
    var result = 0
    for(arg <- args) result += arg
    result
  }
  val num = sum(1 to 5 : _*)*/

  /*var multi : Long = 1;
  for(ch <- "Hello"){
    println(ch.toChar * 1)
    multi *= ch.toChar
  }
  println(multi)*/

//  import scala.collection.mutable.ArrayBuffer
//  val b = ArrayBuffer[Int]()
//  b += 1
//  b += (2,2)
//  b ++= Array(3,3,3)
//  // 移除尾部的几个元素
//  //b.trimEnd(5)
//  b.insert(1, 4, 4, 4, 4)
//  b.remove(1, 4)
//  println(b)


//  val n : Int = 3
//
//  val randomArr = new Array[Int](n)
//  for(i <- 0 until n)
//    randomArr(i) = (new Random).nextInt(n)
//  println(randomArr.mkString("\t"))

//  val a = Array[Int](1, 2, 3, 4, 5, 6)
//  var tmp = 0;
////  for(i <- a.indices if i % 2 == 0 && i!=a.length-1) yield {
////    tmp = a(i)
////    a(i) = a(i + 1)
////    a(i + 1) = tmp
////
////  }
////  println(a.mkString("\t"))
//
//  val n_a = for(i <- a.indices) yield {
//    if(i % 2 == 0 && i!=a.length-1){
//      tmp = a(i)
//      a(i) = a(i + 1)
//      a(i + 1) = tmp
//    }
//    a(i)
//  }
//  println(n_a)


//def arraySort(arr : Array[Int]) = {
//    val buff = ArrayBuffer[Int]()
//    buff ++= (for(ele <- arr if ele >0) yield ele)
//    buff ++= (for(ele <- arr if ele == 0) yield ele)
//    buff ++= (for(ele <- arr if ele < 0) yield ele)
//    buff.toArray
//}

//  val in = new Scanner(new File("/Users/yoreyuan/soft/idea_workspace/work2/flink/flink-demo/flink-examples/scala-note/readme.md"))
//  val worldMap = scala.collection.mutable.Map[String, Int]()
//  while(in.hasNext()){
//    val w = in.next()
//    worldMap(w) = worldMap.getOrElse(w, 0) + 1
//  }
//  for((k,v) <- worldMap){
//    println(k + "\t" + v)
//  }

//  import scala.collection.JavaConversions.propertiesAsScalaMap
//  val props : scala.collection.Map[String, String] = System.getProperties()
//  val keyLens = for(k <- props.keySet) yield k.length
//  val keyMaxLen = keyLens.max
//  for(key <- props.keySet){
//    println(key + " "*(keyMaxLen - key.length) + "|\t" + props(key))
//  }


  //
  /**
    * 枚举类
    *
    * Scala并没有枚举类型，不过，标准类库提供一个Enumeration助手类，可用于产出枚举类
    *
    *
    */
//  object TrafficLightColor extends Enumeration{
//    val Red,
//    Yellow,
//    Green = Value
//  }
//
//  val Red = TrafficLightColor.Green

//  object PokerFace extends Enumeration{
//    type PokerFace = Value
//    val SPADES = Value("♠")
//    val HEARTS = Value("❤")
//    val DIAMONDS = Value("♦")
//    val CLUBS = Value("♣")
//  }
//
//  for(poker <- PokerFace.values){
//    println(poker)
//  }


import java.util.{HashMap => JavaHashMap}

import scala.collection.mutable.HashMap
def transMapValues(javaMap:JavaHashMap[Any,Any]):HashMap[Any,Any]={
  val result=new HashMap[Any,Any]
  for(k <- javaMap.keySet().toArray()){
    println(k -> javaMap.get(k))
    result += k->javaMap.get(k)
  }
  result
}

  val jmap : JavaHashMap[Any, Any] =  new JavaHashMap[Any, Any]();
  jmap.put(1, "one")
  jmap.put(2, "two")
  jmap.put(3, "three")
  println(transMapValues(jmap))
















}
