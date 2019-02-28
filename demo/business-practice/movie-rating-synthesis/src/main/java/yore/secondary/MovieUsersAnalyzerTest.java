package yore.secondary;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 实现二次排序功能
 *
 * Created by yore on 2019/2/22 12:30
 */
public class MovieUsersAnalyzerTest {

    public static void main(String[] args) {
        /**
         * 创建Spark集群上下文sc， 在sc中可以进行各种依赖和参数的设置等，
         * 大家可以通过SparkSubmit脚本的help去看设置信息
         */
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local[2]").setAppName("Movie_Users_Analyzer_Test")
        );

        JavaRDD<String> lines = sc.textFile("demo/business-practice/movie-rating-synthesis/src/main/resources/dataforsecondarysorting.txt");

        JavaPairRDD<yore.secondary.SecondarySortingKey, String> keyvalues = lines.mapToPair(
                new PairFunction<String, yore.secondary.SecondarySortingKey, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<yore.secondary.SecondarySortingKey, String> call(String line) throws Exception {
                        String[] splited = line.split("::");
                        yore.secondary.SecondarySortingKey key = new yore.secondary.SecondarySortingKey(Integer.valueOf(splited[0]), Integer.valueOf(splited[1]));
                        return new Tuple2<yore.secondary.SecondarySortingKey, String>(key, line);
                    }
                }
        );

        // 按Key值进行二次排序
        JavaPairRDD<yore.secondary.SecondarySortingKey, String> sorted = keyvalues.sortByKey(false);

        JavaRDD<String> result = sorted.map(
                new Function<Tuple2<yore.secondary.SecondarySortingKey, String>, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
                        // 取第二个值Value值
                        return tuple._2;
                    }
                }
        );

        List<String> collected = result.take(10);

        for(String item : collected){
            System.out.println(item);
        }


    }
}
