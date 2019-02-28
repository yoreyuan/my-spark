package yore

import java.io.PrintWriter

/**
  * ⌘I 实现方法（实现接口中的方法）
  *
  * Created by yore on 2019/1/24 14:22
  */
object TraitDemo {

  def main(args: Array[String]): Unit = {
    /*val acct = new SavingsAccount(6)
    acct.withdraw(1.2)

    val acct2 = new SavingsAccount2(6) with ConsoleLogger2
    acct2.withdraw(1.2)
    acct2.log("sss")*/

    // 这种定义会报空指针
//    val savingsAccount = new SavingsAccount2 with  FileLogger{
//      val filename = "myapp.log"
//    }
    // 正确的做法是： 提前定义
    /*val acct = new {
      val filename = "myapp.log"
    } with SavingsAccount2 with FileLogger
    acct.log("yore")*/



  }

}

/**
  * 特质可以像Java接口一样使用（无需将方法定义为abstract，特质中未被实现的方法默认就是抽象的）
  *   特质中定义一个抽象方法
  *
  */
trait Logger{
  def log(msg : String)
  def info(msg: String){
    log(s"INFO: $msg")
  }
  def warn(msg : String){
    log(s"WARN: $msg")
  }
  def severe(msg : String){
    log(s"SEVERE: $msg")
  }
}

/**
  * 定义一个实现类，实现特质的抽象方法，
  * 这里用extends，而不是implements
  */
class ConsoleLogger extends Logger{
  override def log(msg: String): Unit = {
    println(msg)
  }
}

/**
  * 还可以将特质的实现类当作一个特质，
  * 这个特质中已经提供一个带有实现的方法
  */
trait ConsoleLogger2 extends Logger{
  override def log(msg: String): Unit = {
    println(msg)
  }
}
trait Account {
  var balance = 0.0
  def withdraw(amount : Double)
}

class SavingsAccount(/*var balance : Double = 3*/) extends Account with ConsoleLogger2{
  override def withdraw(amount: Double): Unit = {
    if(amount > balance)log("Insufficient funds")
    else balance -= amount
    log(balance.toString)
  }
}

/**
  * 带有特质的对象
  *
  * SavingsAccount2是一个抽象类，
  *
  */
abstract class SavingsAccount2(/*var balance : Double = 3*/) extends Account{
  override def withdraw(amount: Double): Unit = {
    /*if(amount > balance)log("Insufficient funds")
    else balance -= amount
    log(balance.toString)*/
    println(balance)
  }
}


class SavingsAccount3(/*var balance : Double = 3*/) extends Account with ConsoleLogger2 with ShortLogger {
  // 子类中的一个普通方法
  var interest = 0.0
  override def withdraw(amount: Double){
    if(amount > balance)log("Insufficient funds")
    else balance -= amount
    log("SavingsAccount3 : " + balance.toString)
  }
}


/**
  * super.log并不像类那样拥有相同的含义。
  * 实际上，super.log调用的是另一个特质的log方法，具体是哪一个特质取决于特质被添加的顺序。
  *
  * 如果需要控制决堤是哪一个特质的方法被调用，则可以在方括号中给出名称：
  *   super[ConsoleLogger].log(...)
  */
trait TimestampLogger /*extends ConsoleLogger*/ extends Logger{
  abstract override def log(msg: String): Unit = {

    super.log(s"${java.time.Instant.now()} $msg")
  }
}


/**
  * 10.8 特质中的具体字段
  *   在JVM中由于一个类智能扩展一个超类，因此来自特质的字段不能以相同的方式继承。
  *   由于这个限制，maxLength是被直接追加到了SavingsAccount3类中，跟interest字段排在一起。
  *
  */
trait ShortLogger extends Logger{
  // 具体的字段
  val maxLength = 15
//  var maxLength2 : Int

  abstract override def log(msg: String) {
    super.log(
      if(msg.length <= maxLength) msg
      else s"${msg.substring(0, maxLength - 3)} ..."
    )
  }
}


/**
  * 10.10 特质构造顺序
  *
  * <b>注意：这里有一个陷阱</b>
  *   在定义filename不能使用具体的值，
  *   例如：这样写行不通：
  *     val filename : String = "app.log"
  *   问题出在构造器顺序上。FileLogger构造器先于子构造器执行。
  *   new语句构造的其实是一个扩展自SavingsAccount（超类）并混入了FileLogger特质的匿名类的实例。
  *   filename的初始化只发生在这个匿名子类中。实际上，在轮到子类之前它根本不会发生，
  *   因为在FileLogger的构造器就会抛出一个空指针异常
  *
  *   解决方法：
  *   ①使用early definition。提前定义发生在常规的构造序列之前。
  *     val acct = new {
  *       val filename = "myapp.log"
  *     } with SavingsAccount with FileLogger
  *
  *   ②或者在定义SavingsAccount类继承后加{提前定义块} with 特质{ 实现 }
  *
  *   ③使用懒值
  *   lazy val out = new PrintStream(filename)
  *
  */
trait FileLogger extends Logger{

  val filename : String /*= "app.log"*/

  // 这是特质构造器的一部分
  val out = new PrintWriter(/*"app.log"*/ filename)

  // 这同样是特质构造器的一部分
  out.println(s"${java.time.Instant.now()}")

  override def log(msg: String): Unit ={
    out.println(msg)
    out.flush()
    println("------" + filename)
  }
}

//class SavingsAccount4 extends  FileLogger with ShortLogger


trait LoggedException extends Exception with ConsoleLogger2{
  // 这个类只能被混入 Exception的子类
  this : Exception =>
    override def log(msg: String): Unit ={
      log(getMessage)
    }
}

/**
  * 混入LoggedException特质的类
  *
  */
class UnhappyException extends LoggedException{
  override def getMessage: String = "arggh!"
}



