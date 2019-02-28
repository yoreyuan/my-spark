package yore.sql

/**
  * 二次排序
  *
  * Created by yore on 2019/1/4 14:21
  */
class SecondarySortKey(val first : Double, val second : Double) extends Ordered[SecondarySortKey] with Serializable {

  override def compare(other : SecondarySortKey): Int = {
    //先判断第一个排序字段是否相等，如果不相等，就直接排序
    if(this.first - other.first != 0){
      (this.first - other.first).toInt
    }else{
      // 如果第一个字段相等，则比较第二个字段，若实现多次排序则以此判断
      if(this.second - other.second >0){
        Math.ceil(this.second - other.second).toInt
      }else if(this.second - other.second < 0 ){
        Math.floor(this.second - other.second).toInt
      }else{
        (this.second - other.second).toInt
      }
    }

  }
}
