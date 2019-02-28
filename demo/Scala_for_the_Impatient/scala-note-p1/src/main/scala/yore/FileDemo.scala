package yore

import java.io.PrintWriter


/**
  * 文件和正则
  *
  * Created by yore on 2019/1/23 09:20
  */
object FileDemo {

  def main(args: Array[String]): Unit = {
    /**
      * 9.1 读取行
      *
      */
    import scala.io.Source
    val filename = "demo/Scala_for_the_Impatient/scala-note-p1/src/main/resources/people.txt"
    val source = Source.fromFile(filename , "UTF-8")

    // 获取行的迭代器
    val lineIterator = source.getLines

    /*for(l <- lineIterator){
      println(l)
    }*/
    //或者直接对迭代器应用toArray或toBuffer方法，将这些行放到数组或者数组缓冲当中：
    /*val lines = lineIterator.toArray
    println(lines.mkString("\n"))*/
    /*val lines = lineIterator.toBuffer
    println(lines.toString)*/
    // 或者直接将这个文件读取重一个字符串. 当文件不是很大时。
    /*val contents = source.mkString
    println(contents)*/


    // 从文件中读取一个单个字符，直接把Source对象当作迭代器。
    /*for(c <- source){
      println(c)
    }*/
    /*val iter = source.buffered
    while (iter.hasNext){
      if('M'.equals(iter.head)){
        println("开始：" + iter.next)
      }else{
        println(iter.next)
      }
    }*/

    // 读取词法单元和数字
    /*val token = source.mkString.split("\\s")
    println(token.mkString("$$"))
    for(s <- token){
      println(s)
    }*/
    source.close()

    // ===

    /*// 从URL或其他源读取
    val source1 = Source.fromURL("http://horstmann.com", "UTF-8")
    // 从给定的自妇产中读取 - 对调试很有用
    val source2 = Source.fromString("Hello, World!")
    val source3 = Source.stdin  // 从标准输入读取
    println(/*source1.mkString*/source2.mkString)*/

    import java.io.File
    import java.io.FileInputStream
    // 读取二进制文件
    /*val file = new File(filename)
    val in = new FileInputStream(file)
    val bytes = new Array[Byte](file.length.toInt)
    in.read(bytes)
    in.close()*/

    // 写入文本文件
    /*val out = new PrintWriter("numbers.txt")
    for(i <- 1 to 100)
      out.println(i)
    out.close()*/


    // 访问目录
    /*import java.nio.file._
    val dirname = "/Users/yoreyuan/soft/idea_workspace/work2/spark/spark-demo-2.x/demo/Scala_for_the_Impatient/scala-note-p1"
    val entries = Files.walk(Paths.get(dirname))
    try{
//      entries.forEach(p => println(p))
    }finally {
      entries.close()
    }*/


    // 序列化
    /**
      * 在Java中声明一个可序列化的类：
      * 实现implements接口，添加serialVersionUID
      *
      * Scala
      * 如果能忍受默认的ID，则可以省略@SerialVersionUID(42L)注解
      * 继承 Serializable
      *
      * Scala集合类都是可序列化的，因此可以把它用作可序列化类的成员：
      *
      */
    /*@SerialVersionUID(42L) class Persion_1 extends Serializable{

    }
    // 序列化对象
    val fred = new Persion_1
    import java.io._
    val out = new ObjectOutputStream(new FileOutputStream("/tmp/test.obj"))
    out.writeObject(fred)
    out.close()
    // 反序列化对象
    val in = new ObjectInputStream(new FileInputStream("/tmp/test.obj"))
    val savedFred = in.readObject().asInstanceOf[Persion_1]*/


    // 进程控制
    /**
      * scala.sys.process包提供了用于与shell程序交互的工具。
      * 可以用Scala编写shell脚本，同时充分利用 Scala提供的所有威力。
      *
      * !操作符返回的结果是被执行程序的返回值 : 程序成功执行的话就是 0，否则就是 表示错误的非0值。
      *
      * 输出重定向到文件
      *    ("ls -al /"#> new File("filel工st.txt")).!
      *
      * 追加到文件末尾，使用 #>>
      *    ("ls -al /etc"#>> new File ("filelist.txt")).!
      *
      * 以某个文件的内容作为输入，使用 #<
      *    ("grep u" #< new File ("filelist.txt")).!
      *
      * 从URL重定向到输入：
      *    import java.net.URL
      *    ("grep Scala"#< new URL("http://horstmann.com/index.html")).!
      *
      * 进程组合使用
      *    p #&& q    如果p成功，则执行q
      *    p #|| q    如果p不成功，则执行q
      *
      *
      * 模板
      *    #!/bin/sh
      *    exec scala "$0" "$@"
      *    !#
      *    Scala命令
      *
      *  说明:从Java程序通过javax. script包的脚本集成功能来运行Scala脚 本。可以这样来获取脚本引擎
      * ScriptEngine engine = new ScriptEngineManager().getScriptEngineByName(”sea la ”)
      *
      */
    import scala.sys.process._
    //!操作符执行的就是这个 ProcessBuilder对象
    // 如果你使用!!而不是!的话，输出会以字符串的形式返回:
//    println("ls -al ..".!!)
//    println( ("ls -al /" #| "grep u").! )



    /**
      * Java代码：
      *
      * Pattern p = Pattern.compile("(\\[)(\\w+)(\\])");
      * String s = "[] [1] [sdfsd9897]";
      * Matcher m = p.matcher(s);
      * while (m.find()){
      *     System.out.println(m.group(2));
      * }
      */
    // \[([^]]+)\]
//    val regex = """\[([^\W])+\]""".r
    val regex = """\[([^]])+\]""".r
    val regex2 = """(\[)(\w+)(\])""".r
    val s = "[] [1] [sdfsd9897]"
    // findAllIn会返回遍历所有匹配项的迭代器。
    for(m <- regex2.findAllMatchIn(s)){
//      print("##\t")
//      println(m.group(2))
    }
//    println(regex.findAllIn(s))


    println(s"""${"-"*6} 正则提取器模式 ${"-"*6}""")
    val regex2(start,midddle , end) = "[1]"
//    println(start, midddle, end)
    for(regex2(start,midddle,end) <- regex2.findAllIn("[] [1] [sdfsd9897]")){
//      println(midddle)
    }


  }

}
