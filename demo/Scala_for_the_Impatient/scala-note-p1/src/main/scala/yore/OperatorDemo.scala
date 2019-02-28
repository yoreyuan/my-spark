package yore

/**
  *
  * Created by yore on 2019/1/28 15:29
  */
object OperatorDemo {

  def main(args: Array[String]): Unit = {


  }


  /**
    * 中置操作符
    *
    *
    */
  class Fraction(n : Int, d : Int){
    private val num = n
    private val den = d

    def *(other : Fraction) = new Fraction(num * other.num, den * other.den)
  }

}
