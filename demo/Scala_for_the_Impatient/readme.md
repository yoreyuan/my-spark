Scala for the Impatient - Cay S. Horstmann
===

Scala既有动态语言那样的灵活简洁，同时又保留了静态类型检查带来的安全保障和执行效率，加上其强大的抽象能力，
既能处理脚本化的临时任务，又能处理高并发场景下的分布式互联网大数据应用，可谓能缩能伸。  

《Programming in Scala》 - Margin Odersky
Scala是一门具备高度表达能力且十分灵活的语言。
它让类库编写者们可以使用非常精巧的抽象，以便类库的使用这门可以简单地、直观地表达自己。

 应用程序开发工程师 | 类库设计人员 | 总体Scala技能层级
 ---- | ---- | ----
 初级(A1) |   |  初级
 中级(A2) | 初级(L1) | 中级
 专家(A3) |   高级(L2)  |   高级  
  | | 专家(L3)    |   专家  
  
  
Scala 2.12新添加的特性，字符串插值、动态调用、隐式类、future
[改进建议、代码连接](http: //horstmann . com/scala)  

### Chapter one

* Scala解释器  
实际上Scala程序并不是解释器，而是快速将输入的内容编译为字节码，然后交由JVM执行。因此大多数Scala程序员更倾向于将它称作为REPL(交互式解释器)
* var和val定义变量  
指定类型：val greeting: String = null;   val greeting: Any = "Hellp";
* 数字类型  
Byte、Char、Short、Int、Long、Float、Double、Boolean  
"Hello".intersect("World") // String隐式转换为了StringOps类
RichInt、RichDouble、RichChar等提供了比Int、Double、Char更多的方法  
BigInt、BigDecimal类
* 操作符和函数
* Scaladoc  
[root package v2.11.8](https://www.scala-lang.org/api/2.11.8/#package)  
如果想使用数值类型可以看RichInt、RichDouble等  
如果想使用字符串，可以看StringOps  
书序函数位于scala.math包下  

在REPL中
```
:help可以看到有用的命令清单
:warnings   给出最近编译警告的详细信息
```
   
### Chapter two
* if表达式有值
* 块也有值--是它最后一个表达式的值
* Scala的for循环就像是"增强版"的Java for循环
* 分号（在绝大多数情况下）是不必需的
* void类型是Unit
* 避免在函数定义中使用return
* 注意别再函数定义中漏掉了=
* 异常的工作方式和Java或C++中基本一样，不同的是你在catch语句中使用模式匹配  
 抛出的对象必须是java.lang.Throwable的子类。和java不同的是Scala没有"受检"异常。  
 throw表达式有特殊的类型`Nothing`。  
* Scala没有检查异常


 val被声明为`lazy`时，它的初始化将被推迟，知道我们首次对它取值。
 懒值对于开销较大的初始化语句而言十分有用。可以把懒值当作介于val和def的中间状态
 


### Chapter three
* 定长数组  
若长度固定则用Array，若长度可能有变化则使用ArrayBuffer  
提供初始值时不要使用new ; 用()访问元素；for(elem <- arr if ...) yield ...将原数组转为新数组  
Java数组和Scala数组可以互相转换，用ArrayBuffer,使用`scala.collection.JavaConversion`中的转换函数  
val nums = new Array\[Int](10);  val s = Array("Hello", "World");    

* 变长数组：数组缓冲
`import scala.collection.mutable.ArrayBuffer`  
ArrayBuffer - Array         **toArray**  
Array       - ArrayBuffer   **toBuffer**  

* 遍历数组和数组换从  
until（后面可以跟 by挑几个元素）方法和to方法很像， 只不过它排除了最后一个元素。  

* 数组转换
* 常用算法
* 解读Scaladoc
* 多维数组  
val matrix = Array.ofDim\[Double](3,4)  
matrix(row)(colum)  

* 与Java的互操作  
Scala不会将Array\[String]转换为Array\[Object], 
可以做一个强制转换， java.util.Arrays.binarySearch(a.asInstanceOf\[Array\[Object]], "beef")
  
Scala执行二分查找  
```
val a = Array("Mary", "a", "had", "lamb", "little")  

import scala.collection.Searching._
val result = a.search("beef")

import scala.collection.JavaConversions

/* 
* scala的ArrayBuffer转为java的List
*/
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ArrayBuffer
val command = ArrayBuffer("ls", "-al", "/home/cay")
val pb = new ProcessBuilder(command) // Scala到Java的转换

/* 
* Java的LIst转为scala的Buffer
*/
import scala.collection.JavaConversion.asScalaBuffer
import scala.collection.mutable.Buffer
val cmd : Buffer[String] = pb.command() // Java到Scala的转换
```


### Chapter four
* 构造映射  
val source = Map("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8)  
val source = scala.collection.mutable.Map("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8)  
val source = scala.collection.mutable.Map\[String, Int]()  

* 获取映射中的值  
sources.getOrElse("key", 0) //如果有值返回，没有返回0  
source.get(key) //返回Option对象（要么是Some,要么是None）  

* 更新映射中的值  
source("Bob") = 10  
source += ("Bob" -> 10, "Fred" -> 7)  
移除某个值   source -= "Alice"  
不可变map更新    source + （"Bob" -> 11）  

* 迭代映射
for((k,v) <- map)  

* 已排序映射  
如果需要按照顺序依次访问映射中的键，可以使用`SortedMap`  
val sources = scala.collection.mutable.SortedMap("Allice" -> 10)  
如果要按照插入的顺序方位所有键，则使用`LinkeddHashMap`  
val months = scala.collection.mutable.LinkedHashMap("January" -> 1, "March" -> 3)  

* 与Java的互操作 
```
import scala.collection.JavaConversions.mapAsScalaMap  
val sources : scala.collection.mutable.Map[String, Int] = new java.util.TreeMap[String, Int]  

//从java.util.Properties到Map:  
import scala.collection.JavaConversions.propertiesAsScalaMap  
val props: scala.collection.Map[String, String] = System.getProperties()  

//Scala映射到Java方法
import scala.collection.JavaConversions.mapAsJavaMap  
import java.awt.font.TextAttribute._
val atts = Map(FAMILY -> "Serif", SIZE -> 12)
val font = new java.awt.Font(attrs)
``` 
  
* 元组  
和数组不同，元组是从1开始的  
val t = (1, 3.14, "Fred")  
val (first, second, _ ) = t  

* 拉链操作  
使用元组的原因之一是把多个值绑定在一起，以便它们能够被一起处理，这通常可以用zip方法来完成。  
```
val sysbols = Array("<", "-", ">")
val counts = Array(2, 10, 2)
val pairs = sysbols.zip(counts)

//可以转换成一个Map映射
keys.zip(values).toMap 
```


### Chapter five
* 简单类和无参方法  
调用无参方法可以加上括号，也可以不加  

* 带getter和setter的属性
getter  ->  =  例如：.age
setter  ->  _=  例如：age_=
如果字段是私有，则getter和setter方法也是私有  
如果字段是val，则只有getter方法被生成  
如果你不需要任何getter或setter，可以将字段声明为**private\[this]**.  

* 只带getter的属性
* 对象私有字段  
在Scalal中，方法可以访问该类的所有独享的私有字段  

* Bean属性  
@BeanProperty var name : String = _  
如果是以构造器参数的方式定义了某字段，并且需要JavaBean版的getter和setter方法，
class Persion(@BeanProperty var name : String)  

* 辅助构造器  
辅助构造器的名称必须为this  
必须以一个对先前的已经定义的其他辅助构造器的调用开始。  

* 主构造器  
在Scala中，每个类都有主构造器，主构造器并不比this方法定义，而是与类定义交织在一起。  
    * ①. 主构造器的参数直接放置在类名之后。  class Persion(val name : String, val age: Int ){}  
    * ②. 主构造器会执行类定义中的所有语句。  
    * ③. 主构造器私有， 可以在前面添加private。 class Person private(val id: Int){...}  

* 嵌套类  
在Scala中，几乎可以在任何的语法结构中内嵌任何的语法结构。
比如可以在函数中定义函数。在类中定义类。  


### Chapter sex
* 单例对象  
Scala没有静态方法或者静态字段，但可以用object这个语法结构来达到同样的目的。  
对象本质上可以拥有类的所有特性，它甚至可以扩展其他类或特质。
但也只有一个例外：不能提供构造器参数。  
    * 作为存放工具函数或常量的地方
    * 高效地共享单个不可变实例
    * 需要用单个实例来协调某个服务时。

* 半生对象  
在Java或C++中，通常会用到既有实例方法又有静态方法的类。
在Scala中可以通过类和与类同名的"伴生（companion）"对象来达到同样的目的。  

* 扩展类或特质的对象
* apply方法  
通常，一个apply()方法返回的是半生类的对象  

* 应用程序对象  
除了每次自己提供main方法，我们还可以扩展**App**特质。  
App特质扩展自另个一个特质DelayedInit。所有带有该特质的类，其初始化方法都会被挪到delayedInit方法中。

* 枚举  
Scala并没有枚举类型，不过，标准类库提供一个Enumeration助手类，可用于产出枚举类  


### Chapter seven
Scala总是隐式的引入  
```
import java.lang._
import scala._
// 它包含了常用的类型、隐式转换、工具方法
import Predef._
```

* 包  
```
通过com.horstmann.Employee访问Employee
package com{
    package horstmann{
        class Employee
    }
}
```
* 作用域规则  
在Scala中，包名时相对的；在Java中包名总是绝对的  
解决Scala包名相对引入时的奇怪问题，可以对发生编译发生错误的包使用绝对报名_root_，例如
val subordinates = new **_root_**.scala.collection.mutable.ArrayBuffer\[Employee]  
另一种做法就是"串联式"包语句  

* 串联式包语句  
```
// 这样在{}中的不再能够访问com和com.horstmann.collection，达到了限定成员可见
package com.horstmann.impatient{
  package people{
    class Persion
  }  
}
```
* 文件顶部标记法  
等同于上面的
```
package com.horstmann.impatient
package people
class Persion
```

* 包对象  
包可以包含类、对象、特质，但不能包含函数或变量的定义。  
每个包都可以有一个包对象，在父包中定义它，且名称与子包一样。 例如：
```
package com.horstmann.impatient
package object people{
    // 无需加限定词
    val defaultName = "John Q.Public"
}
package people{
 class Persion{
    // 从包对象拿到的常量
    var name = defaultName
 }
}
```
* 包可见性 
```
package com.horstmann.impatient.perple

class Persion{
    // 在自己的包中可见
    private[people] def description = s"A persion with name $name"
    ...
    
    // 亦可以延伸到上层
    private[impatient] def description = s"A persion with name $name"
}
```  

* 引入  
引入某个包下的全部成员： import java.awt._  
引入类或对象的所有成员：    import java.awt.Color._  

* 任何地方都可以声明引入  
import语句作用的效果一直延伸到包含改语句的块末尾  

* 重命名和隐藏方法
如果仅想引入包中的几个成员，可以使用选取器：  
import java.awt.{Color, Font}  
还允许重命名选到的成员：(这样HashMap则对应为scala的HashMap，JavaHashMap就是java.util.HashMap)  
import java.util.{HashMap => JavaHashMap}  
import scala.collection.mutable._  
隐藏(这样Hash就被指向了scala的，Java的HashMap被隐藏)  
import java.util.{HashMap => _, _}
import scala.collection.mutable._  

* 隐式引入  


### Chapter eight   继承
extends、final关键字  
重写方法时必须用override

* 扩展类  
和Java一样，使用`extends`关键字；  也可以使用final，这样它就不能被继承了。还可以单个方法加final，这样这个方法就不能被重写  

* 重写方法  
使用override修饰， 
在Scala中调用超类的方法和Java完全一样，使用`super`关键字   

* 类型检查和转换  
判断某个对象是否属于跟定的类，可以使用`isInstanceOf`方法，如果true，就可以使用`asInstanceOf`方法将引用类型转换为子类型的引用。  
如果想要测试p指向的是一个Employee对象但又不是其子类的话，可以用：
`if(p.getClass == classoOf[Employee])`  
  

* 受保护字段和方法  
protected与Java不同的是，protected的成员对于类所属的包而言，是不可见的。  
Scala还提供了一个`protected[this]`变体，将访问权限定在当前的对象。  

* 超类构造  
子类的辅助构造器最终会调用主构造器，只有主构造器可以调用超类的构造器。  
在Scala的构造器中，我们不能调用supper(params)  
```
// 示例：
// 其中Employee类有三个参数：name、get、salary，其中两个被"传递"到了超类。
class Employee(name: String, age: Int, val salary : Double) extends Person(name, age)

// 其等价于Java如下代码
public class Employee extends Person{
    private double salary;
    public Employee(String name, int age, double salary){
        super(name, age)
        this.salary = salary
    }
}
```

* 重写字段  
注意限制如下：  
    * def只能重写另一个def
    * val只能重写另一个val或不带参数的def
    * var只能重写另一个抽象的var  

* 匿名子类  

* 抽象类  
可以用**abstract**关键字来标记不能被实例化的类  
[例如 line:71 Person5](scala-note-p1/src/main/scala/yore/Person.scala)

* 抽象字段  
抽象字段是一个没有初始值的字段。  

* 构造顺序和提前定义  
当我们子类中重写val并且在超类中使用该值时，会出现一个问题，[更多 line：83-120](scala-note-p1/src/main/scala/yore/Person.scala)

* Scala类继承关系  
所有其他类都是AnyRef的子类。当编译大Java虚拟机时，anyRef就是java.lang.Object类同义  
AnyVal和AnyRef都扩展自Any类，而Any类是整个类继承关系中的根节点。  
其中长常用到的`isInstanceOf`和`asInstanceOf`定义在Any类中，  

Nothing类型没有实例。空列表Nil的类型是List(Nothing)，它是List\[T]的子类型，T可以是任何类。???方法被声明为返回类型Nothing。  
void --> Unit  
当摸个方法的参数类型为Any或AnyRef，且用多个入参调用时，这些入参会被放置到元组中。  

* 对象相等行  
[重写equals方法的两种方式 line：123-141](scala-note-p1/src/main/scala/yore/Person.scala)

* 值类  
值类型的设计是为了做高效的隐式转换，不过也可以用它来实现自己的无额外开销的"小微类型"，
    * 扩展自AnyVal
    * 构造器有且只有一个参数，该参数是一个val，且没有方法体
    * 没有其他字段或构造器
    * 自动提供的equals与hashCode方法比较和hash背后对应的那个值。  
    
如果你想要让值类实现某个特质，对应的特质必须显示扩展Any，并且不能有字段，这样的特质称为**全称特质(universal trait)**  

    

### Chapter nine   文件和正则表达式
* 读取行  
* 读取字符  
* 读取词法单元和数字  
* 从URL或其他源读取  
* 读取二进制文件  
* 写入文本文件  
* 访问目录  
* 序列化  
* 进程控制  
* 正则表达式  
* 正则表达式组  

[代码请查看 FileDemo.scala](scala-note-p1/src/main/scala/yore/FileDemo.scala)



### Chapter ten   特质
* 1 为什么没有多重继承  
特质可以同时拥有抽象方法和具体方法以及状态；类可以实现多个特质  

* 2 当作接口使用的特质
    * 特质的关键字：`trait`  
    * 实现特质使用：`extends`  
    * 添加多个特质：`with`  

* 3 带有具体实现的特质  
在Scala中特质中的方法不需要一定是抽象的。  

* 4 带有特质的对象  

* 5 叠加在一起的特质  
我们可以为类或对象添加多个互相调用的特质，

* 6 在特质中重写抽象方法
当我们在子特质中调用父类的方法（比如：super.log()）时因为父类的log方法没有具体实现，理论上这个类就不能编译了，
如果需要让Scala认为这个类依旧是抽象的，需要混入一个具体的log方法，必须将重写的方法前加上`abstract override`    

* 7 当作富接口使用的特质  
特质可以包含大量工具方法，而这些工具方法可以依赖一些抽象方法来实现。

* 8 特质中的具体字段  
在特质中字段可以是具体的，也可以是抽象的。如果给出了初始值的，那么字段就是具体的。  
**注意：**当我们扩展摸个自来然后修改超类是，子类无需重新编译，因为虚拟机知道继承是怎么回事。
不过当特质改变时，所有混入该特质的类都必须被重新编译。

* 9 特质中的抽象字段  
特质中未被初始化的字段在具体的子类中必须被重写  

* 10 特质构造顺序  
和类一样，特质也可以由构造器，其由字段的初始化和其它特质中的语句构成。  
构造器执行的顺序：  
    * 首先调用超类的构造器
    * 特质构造器在超类构造器之后、类构造器之前执行
    * 特质由左到右被构造
    * 在每个特质当中，父特质先被构造
    * 如果多个特质共有一个父特质，而那个父特质已经被构造，则该父特质不会被再次构造
    * 所有的特质构造完毕，子类被构造  

```
例子： class SavingsAccount extends Account with FileLogger with ShortLogger
Account（超类）
Logger（第一个特质的父特质）
FileLogger（第一个特质）
ShortLogger（第二个特质）注意因为它的父特质Logger已经被构造
SavingsAccount（类）

线性化描述：(串联并去掉重复项，右侧胜出)
lin(SavingsAccount)
= SavingsAccount >> lin(ShortLogger) >> lin(FileLogger) >> lin(Account)
= SavingsAccount >> (ShortLogger >> Logger) >> (FileLogger >> Logger) >> lin(Account)
= SavingsAccount >> ShortLogger >> FileLogger >> Logger >> Account

```    

* 11 初始化特质中的字段  
特质不能有构造器参数。每个特质都有一个无参的构造器。
例如这样写是错误的： val acct = new SavingsAccount with FileLogger("myapp.log")  
解决方法：
①使用提前定义（early definition）
②使用懒值 lazy 。因为懒值在每次使用前都会检查是否已经初始化，所以它的效率不高

* 12 扩展类的特质  
特质可以扩展另一个特质，而又特质组成的继承层级很常见。
不是很常见的用法是，特质可以扩展类。这个类会自动成为所有混入该特质的超类。

* 13 自身类型  
当特质以如下代码开始定义时：
  this : Exception =>  
自身类型的特质可以解决特质建的循环依赖。  
自身类型同样可以处理**结构类型**，这种类型只给出类必须拥有的方法，而不是类的名称。
```
trait LoggedException extends Exception with ConsoleLogger2{
  // 这个特质可以被混入任何拥有 getMessage方法的类
  this : {def getMessage() : String } =>
    override def log(msg: String): Unit ={
      log(getMessage)
    }
}
```  

* 14 背后发生了什么  
Scala需要将特质翻译成JVM的类和接口。  
    * 只有抽象方法的特质被简单的编程一个Java接口。
    * 特质的方法对应的是Java的默认方法
    * 如果特质有字段，对应的Java接口就有getter和setter方法。


[代码请查看 TraitDemo.scala](scala-note-p1/src/main/scala/yore/TraitDemo.scala)



### Chapter eleven   操作符
主要内容包括：  
    * 标识符由字母、数字或运算符构成；
    * 一元和二元操作符其实是方法调用
    * 操作符优先级取决于第一个字符，而结合性取决于最后一个字符
    * apply和update方法在对expr(args)表达式求值时被调用。
    * 提取器从输入中提取元组或值的序列
    * 扩展自Dynamic特质的类型可以在运行期检视方法名和入参
    
* 标识符  
变量、函数、类的名称通通成为标识符。也可以使用Unicode字符。可以使用反单引号，例如：Thread.`yield`()  

* 中置操作符  
例如： 1 to 10 实际这个表达式是一个方法调用： 1.to(10)  
这样的表达式成为**中置**，因为操作符位于两个参数之间。  

* 一元操作符  
中置操作符是二元的，它有两个参数。只有一个参数的操作符被称为一元操作符。  
一元操作符都被我们转换成了 `unary_操作符` 的方法进行调用  
如果某一元操作符跟在后面，它就是后值操作符，a 标识符 等同于`a.标识符`  
**注意:** 如果在使用后置操作符时报编译器的错误，可以在源码中添加`import scala.language.postflxOps`  

* 赋值操作符  

* 优先级  
首先执行高优先级的操作符  
后置操作符的优先级低于中置操作符 

* 结合性
当我们有一系列相同优先级的操作符时，操作符的结合性确定了它们是从左到右求值还是从右到左求值。  
在Scala中所有的操作符都是左结合的，除了：
    * 以冒号(:)结尾的操作符
    * 赋值操作符
    * 用于构造列表的双冒号(::)操作符  

* apply和updata方法  
 这个机制也应用于数组和映射：
 ```scala
 val scores = new scala.collection.mutable.HashMap[String, Int]
 // 调用scores.update("Bob", 100)
 scores("Bob") = 100
 // 调用scores.apply("Bob")
 val bobsScore = scores("Bob")
 ```
apply方法通常用在伴生对象中，用来构造对象，而不用显示的new，例如：
```scala
class Fraction(n: Int, d: Int){

}

/**
* 因为有Fraction对象有apply方法，我们可以使用Fraction(3, 4)构造一个分数，
* 而不用new Fraction(3, 4)
*/
object Fraction {
  def apply(n: Int, d: Int): Fraction = new Fraction(n, d)
  
  def unapply(input: Fraction): Option[(Int, Int)] = None/*{
    if(input.d == 0){
      None
    }else Some{
      (input.n, input.d)
    }
  }*/
}

```

* 提取器  
带有unapply方法的对象。  
你可以把unapply方法当作伴生对象中apply方法的反向操作 。
apply方法接受构造参数，然后将它们变成对象 。 
而unapply方法接受一个对象，然后从中提取值一一通常这些值就是当初用来构造该对象的值。  

* 带单个参数和无参数的提取器  
```scala
//提取器也可以只是测试其输入而并不真的将值提取出来 。这样的话，unapply方法应返回 Boolean0 例如:
object IsCornpound {
  def unapply(input: String): Option[Boolean] = input.contains(" ")
}

// 你可以用这个提取器给模式增加一个测试，例如:
/*author match {
case Name (first , IsCompound()) =>
//如果姓氏是组合饲则匹配，比如van der Linden case Narne(first , last) => ……
}
*/
```

* unapplySeq方法  
要提取任意长度的值的序列，我们应该用 unapplySeq来命名我们的方法 。
它返回 一个Option [Seq [A] l ，其中A是被提取的值的类型 。 
```scala
// 举例来说， Name提取器可以产出 名字中所有组成部分的序列:
object Name {
  def unapplySeq(input : String) : Option[Seq[String]] =
  if (input.trim == "") None else Some(input.trim.split("\\s+"))
}
```

* 动态调用  
如果某个类型扩展自`scala.Dynamic`这个特质，那么它的方法调用、getter和setter等都会被重写成对特殊方法的调用，
这些特殊的方法可 以检视原始调用的方法名和参数，并采取任意的行动 。



### Chapter twelve   高阶函数  
Scalar昆合了面向对象和函数式的特性  
 
* 作为值的函数  


* 匿名函数
* 带函数参数的函数
* 参数(类型)推断
* 一些有用的高阶函数
* 闭包
* SAM转换
* 柯里化
* 控制抽象
* return表达式









### Chapter fourteen   模式匹配和样例类  
模式匹配是一个十分强大的机制，可以应用在很多场合：switch语句、类型查询，以及"析构"（获取复制表达式中的不同部分）。
样例类针对模式匹配进行了优化。  

* 更好的switch

* 守卫
* 模式中的变量
* 类型模式
* 匹配数组、列表和元组
* 提取器
* 变量声明中的模式
* for表达式中的模式
* 样例类
* copy方法和带名参数
* case语句中的中置表示法
* 匹配嵌套结构
* 样例类是邪恶的吗
* 密封类
* 模拟枚举
* Option类型
* 偏函数

 



* 如果没有
* 带函数参数的函数
* 参数(类型)推断
* 一些有用的高阶函数
* 闭包
* SAM转换
* 柯里化
* 控制抽象
* return表达式















