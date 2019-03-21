package yore;

import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import yore.comm.PropertiesUtil;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yore on 2019/3/20 13:40
 */
public class JavaLogQuery {
    public static final List<String> exampleApacheLogs = Lists.newArrayList(
            "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)\" \"UD-1\" - \"image/jepg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" 62.24.11.25 images.com 1368492167 - whatup",
            "10.10.10.110 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)\" \"UD-1\" - \"image/jepg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" 62.24.11.25 images.com 1368492167 - whatup",
            "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg HTTP/1.1\" 304 306 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; Window NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 3.5.30729; Release=APP)\" \"UD-1\" - \"image/jepg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" 0 73.23.2.15 images.com 1358492557 - whatup"
    );

    public static final Pattern apacheLogRegex = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\" .*"
    );


    public static class Stats implements Serializable{
        private final int count;
        private final int numBytes;

        public Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }

        public Stats merge(Stats other){
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "count=" + count +
                    ", numBytes=" + numBytes +
                    '}';
        }
    }


    public static Tuple3<String, String, String> extractKey(String line){
        Matcher m = apacheLogRegex.matcher(line);
        if(m.find()){
            String ip = m.group(1);
            String user = m.group(3);
            String query = m.group(5);

            if(!"-".equalsIgnoreCase(user)){
                return new Tuple3<>(ip, user, query);
            }
        }
        return new Tuple3<>(null, null, null);
    }

    public static Stats extractStats(String line){
        Matcher m = apacheLogRegex.matcher(line);
        if(m.find()){
            int bytes = Integer.parseInt(m.group(7));
            return new Stats(1, bytes);
        }
        return new Stats(1, 0);
    }


    /**
     * Spark主程序代码
     *
     * @param args
     */
    public static void main(String[] args) {
        // 设置日志级别
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf()
                .setAppName(PropertiesUtil.getPropString("spark.app.name"))
                .setMaster(PropertiesUtil.getPropString("spark.master"));
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataSet = (args.length == 1)? jsc.textFile(args[0]) : jsc.parallelize(exampleApacheLogs);

        JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet.mapToPair(
                new PairFunction<String, Tuple3<String, String, String>, Stats>() {
                    @Override
                    public Tuple2<Tuple3<String, String, String>, Stats> call(String line) throws Exception {
                        return new Tuple2<Tuple3<String, String, String>, Stats>(extractKey(line), extractStats(line));
                    }
                }
        );

        JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(
                new Function2<Stats, Stats, Stats>() {
                    @Override
                    public Stats call(Stats v1, Stats v2) throws Exception {
                        return v1.merge(v2);
                    }
                }
        );

        List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
        for(Tuple2<?, ?> t : output){
            /**
             * (10.10.10.10,"FRED",GET http://images.com/2013/Generic.jpg HTTP/1.1)	Stats{count=2, numBytes=621}
             * (10.10.10.110,"FRED",GET http://images.com/2013/Generic.jpg HTTP/1.1)	Stats{count=1, numBytes=315}
             */
            System.out.println(t._1() + "\t" + t._2());
        }

        jsc.stop();

    }

}
