package yore.secondary;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 自定义Key值类
 *
 * Created by yore on 2019/2/22 12:30
 */
public class SecondarySortingKey implements Ordered<SecondarySortingKey>, Serializable {
    private int first;
    private int second;

    public SecondarySortingKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondarySortingKey that) {
        // 如果第一个值不相等，直接比较第一个值即可，如果第一个相等，则比较第二个值
        if(this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        }else{
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortingKey that) {
        if(this.first < that.getFirst()){
            return true;
        }else if(that.first == that.getFirst() && that.second < that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortingKey that) {
        if(SecondarySortingKey.this.$less(that)){
            return true;
        }else if(that.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortingKey that) {
        if(this.first > that.getFirst()){
            return true;
        }else if(that.first == that.getFirst() && this.second > that.getSecond()){
            return true;
        }
        return false;
    }



    @Override
    public boolean $greater$eq(SecondarySortingKey that) {
        if(SecondarySortingKey.this.$greater(that)){
            return true;
        }else if(that.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortingKey that) {
        if(this.first - that.getFirst() !=0){
            return this.first - that.getFirst();
        }else{
            return this.second - that.getSecond();
        }
    }


    public int getFirst() {
        return first;
    }
    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }
    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortingKey that = (SecondarySortingKey) o;
        return first == that.first &&
                second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
//        return Objects.hash(first, second);
    }
}
