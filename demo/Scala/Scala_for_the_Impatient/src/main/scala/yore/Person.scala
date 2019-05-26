package yore

/**
  *
  * Created by yore on 2019/1/21 14:10
  */
class Person {
  var age = 0

}

/**
  * 只读属性的：
  * 只有getter，但没有setter
  */
class Person2{
  // 私有的final字段，只有getter方法，
  val timeStamp = java.time.Instant.now

  private var value = 0
  def increment(): Unit ={
    value += 1
  }
  def current = value

}

class Counter{
  private var value = 0
  def increment(): Unit = {
    value += 1
  }

  // 这里之所以访问other.value是合法的，是因为other也同样是Counter对象。
  def isLess(other : Counter) = value < other.value

  // Scala允许我们定义更加严格的访问限制，通过private[this]这个修饰符来实现。
  private[this] var value2 = 0;
  // 此时就会无法调用赋值。
  //def isLess2(other : Counter) = value2 < other.value2

}

/**
  * Bean规范：
  */
import scala.beans.BeanProperty
class Person3{

  @BeanProperty var name : String = _

}

/**
  * 重写字段
  */
class Persion4(val name : String) {
  override def toString = s"${getClass.getName}[name=$name]"
}
class SecretAgent(codename : String) extends Persion4(codename){
  // 不想再暴露真名......
  override val name = "secret"
  // ......或类名
  override val toString = "secret"
}


/**
  * 抽象类
  * 如果某个类至少存在一个抽象方法，则该类必须声明为abstract
  *
  */
abstract class Person5(val name: String){
  // 这个方法没有方法体，因此没有实际的意义，
  def id : Int

  // 抽象字段
  var nu : String

}


/**
  * 构造顺序和前提定义
  *
  * 在Scala类里面，处理方法和字段的定义以外的代码，全都是主构造器的内容。
  * 辅助构造器(从构造器)的定义，def this开始。从构造器的都会直接或间接调用主构造器。
  *
  * 例如动物类中定义一个可感知的距离，为前方10个单位那么远
  *
  */
class Creature{
  val range : Int = 10
  val env : Array[Int] = new Array[Int](range)
}

/**
  * 不过蚂蚁是近视的：
  *
  * 当调用Ant类的env时会发现被初始化为了0，
  * 原因：。。。
  *
  * 为解决这个问题，常用的解决方式为：
  * ①将val声明为final、这样很安全但并不灵活
  * ②在超类中将val声明为lazy、这样很安全但并不高效
  * ③在子类中使用前提定义语法
  *
  * <b>前提定义语法</b>
  * 可以让我们在超类的构造器执行之前初始化子类的val字段。
  * 但是这个语法简直难看到家了，估计没人会喜欢
  * {{{
  *   class Ant extends {override val range = 2} with Creature
  * }}}
  *
  * 问题的根本原因来自于Java语言的一个设计决定 -- 即允许在超类的构造方法中调用子类的方法。
  *
  */
class Ant extends Creature{
  override val range = 2
}


/**
  * 对象的相等性
  * 重写equals方法，
  *
  * 当重新定义equals时，记得同时也定义hashCode。
  */
class Item(val description: String, val price: Double){
  // 第一种： 重写equals方法
  /*override def equals(other: Any): Boolean = {
    other.isInstanceOf[Item] && {
      val that = other.asInstanceOf[Item]
      description == that.description && price == that.price
    }
  }*/

  // 第二种：是用匹配模式
  override def equals(other: Any): Boolean = other match {
    case that : Item => description == that.description && price == that.price
    case _ => false
  }

  // ##获取元组的hashCode，但这个是安全的，当hashCode为null时返回0
  override def hashCode(): Int = (description, price).##
}


/**
  * 值类
  *
  * 军事时间（military time）
  *
  * 创建一个对象后，我们可以调用里面的方法。我们也可以直接调用这个对象，对象会返回背后对应的值，但是这个值对象不能调用Int的方法
  *
  * 为确保正确的初始化，将构造器做成私有的，并在半生对象中提供一个工厂方法
  *
  * @param time
  */
class MilTime private(val time : Int) extends AnyVal{
  def minutes = time % 100
  def hours = time / 100

  override def toString: String = f"$time%04d"
}
object MilTime{
  def apply( t: Int ): MilTime = if(0 < t && t < 2400 && t % 100 < 60) new MilTime(t)
  else throw new IllegalArgumentException
}


object PersonTest{
  def main(args: Array[String]): Unit = {
    /*val person = new Person
    println(person.age)
    person.age = 11
    println(person.age)*/

    /*val person2 = new Person2
    println(person2.timeStamp)
    println(person2.current)
    person2.increment()
    println(person2.current)*/

    /*val person3 = new Person3
    person3.setName("yore")
    println(person3.getName)*/

    /*val creature = new Creature
    println(creature.env.length)
    val ant = new Ant
    //
    println(ant.env.length)*/

    // -900538381
//    println((null).##)

    val lunch : MilTime = MilTime(1230)
    println(lunch.minutes)
    println(lunch.hours)
    println(lunch)
//    println(lunch * 2)


  }
}