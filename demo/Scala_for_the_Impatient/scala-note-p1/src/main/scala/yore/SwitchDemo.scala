package yore

/**
  * 14章： 模式匹配和样例类
  *   * match表达式是一个更好的switch，不会有意外掉入下一个分支的问题
  *   * 如果没有模式匹配，会抛出MatchError。可以使用case _模式来避免
  *   * 模式可以包含一个随意定义的条件，乘坐守卫
  *   * 可以对表达式的类型进行匹配；优先选择模式匹配恶如不是isInstanceOf / asInstancOf
  *   * 可以匹配数组、元组和阳历类的模式，然后将匹配到的不同部分绑定到变量
  *   * 在for表达式中，不能匹配的情况会被安静跳过
  *   * 样例类是编译器会为之自动产出模式匹配所需要的方法类
  *   * 样例类继承层级中的公共超类应该是sealed的
  *   * 用Options来存放对于可能存在也可能不存在的值-这比null更安全
  *
  * Created by yore on 2019/2/25 16:39
  */
object SwitchDemo {

  def main(args: Array[String]): Unit = {
    var sign = 0
    val ch : Char = '-'

    /*ch match {
      case '+' => sign = 1
      case '-' => sign = -1
      case _ => sign = 0
    }*/

    sign = ch match {
      case '+' | '×' => 1
      case '-' | '÷' => -1
      case _ => 0
    }


    println(sign)
  }

}
