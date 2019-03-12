package yore.dt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import yore.db.*;

import java.sql.ResultSet;
import java.util.*;

//import org.apache.spark.sql.hive.HiveContext;
//import kafka.serializer.StringDecoder;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * 在线处理广告点击，广告点击的基本数据格式：
 *  timestamp、ip、userID、adID、province、city
 *
 * @see <a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html">streaming-programming-guide.html</a>
 * @see <a href="http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html">streaming-kafka-0-10-integration.html<a/>
 * <br/>
 *
 *
 * Created by yore on 2019/3/5 17:49
 */
public class AdClickedStreamingStats {

    public static void main(String[] args) throws InterruptedException {

        /**
         * 第一步，配置SparkConf
         *  ①至少2个线程，因为spark Streaming应用程序在运行的时候，至少有一条线程用于不断地循环接收数据，
         *  并且至少有一个线程用于处理接收的数据（否则无法有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）
         *
         *  ②对于集群而言，每个executor一般不知一个Thread，那对于处理Spark Streaming的应用程序而言，每个Executor
         *  一般分配多少Core比较合适？根据经验，5个左右的Core最佳（一般分配为奇数个Core表现最佳，如3个、5个、7个Core等）
         *
         */
        SparkConf conf = new SparkConf()/*.setMaster("spark://cdh3:7077")*/.setMaster("local[2]")
                .setAppName("YORE-20190306-AdClickedStreamingStats")
                /*.setJars(new String[]{
                        "/usr/local/lib/mysql-connector-java-5.1.47.jar",
                        "/usr/local/lib/spark-streaming-kafka_2.11-1.6.1.jar"
                })*/;

        /**
         * 第二部： 创建SparkStreamingContext
         * ① 这是SparkStreaming应用程序所有功能的起始点和程序调度的核心，它的构建可以基于SparkConf参数，也可以基于持久化的
         * SparkStreamingContext的内容来恢复（典型的场景是Driver崩溃后重新启动，
         * 当Driver重新启动后继续之前系统的状态，此时的状态恢复需要基于曾经的Checkpoint）
         *
         * ②在一个Spark Streaming应用程序中，可以创建若干个SparkStreamingContext对象，
         * 使用下一个SparkStreamingContext之前需要把前面正在运行的SparkStreamingContext对象关闭掉。
         *
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        jsc.checkpoint("demo/business-practice/E-commerce_click_real-time_stream/src/main/resources/checkpoint/");


        /**
         * 第三部：创建Spark Streaming输入数据来源input Streaming；
         * ①数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
         *
         * ②这里我们指定的数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直坚挺该端口的数据（当然，改端口服务首先必须存在）
         * 并且在后续会根据业务需要不断有数据产生
         *
         * ③如果经常间隔5s没有数据，不断地启动空的Job其实会造成调度资源的浪费，因为并没有数据需要发生计算，
         * 所以实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有，就不再提交Job
         *
         */
        Map<String, Object> kafkaParams = new HashMap<>();
        //kafakParamters.put("metadata.broker.list", "cdh3:9092,cdh4:9092,cdh5:9092");

        /*JavaPairInputDStream<String, String> adClickedStreaming = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafakParamters,
                topics
         );*/

        /** 2.4.0版本 */
        kafkaParams.put("bootstrap.servers", "cdh3:9092,cdh4:9092,cdh5:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        Set<String> topics = new HashSet<String>(){{
            add("AdClicked");
        }};


        JavaInputDStream<ConsumerRecord<String, String>> adClickedStreaming = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String> filteredadClickedStreaming = adClickedStreaming.transformToPair(
                new Function<JavaRDD<ConsumerRecord<String, String>>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                        /**
                         * 在线黑名单过滤思路：
                         * ①从数据库中获取黑名单转换成RDD，即新的RDD实例封装黑名单数据；
                         *
                         * ②吧代表黑名单的RDD实例和BatchDuration产生的RDD进行Join操作，
                         * 准确的说，是进行leftOuterJoin操作，也就是使用BatchDuration产生的RDD和代表黑名单的RDD的实例进行leftOuterJoin操作，
                         * 如果两者都有内容，就会是true，否则就是false：我们要留下的leftOuterJoin操作结果，为false
                         *
                         */
                        final List<String> blackListNames = new ArrayList<>();
                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                        jdbcWrapper.doQuery("SELECT * FROM blacklisttable", null, new ExecuteCallBack(){

                            @Override
                            public void resultCallBack(ResultSet result) throws Exception {
                                while (result.next()){
                                    blackListNames.add(result.getString(1));
                                }
                            }
                        });

                        List<Tuple2<String, Boolean>> blackListTuple = new ArrayList<Tuple2<String, Boolean>>();
                        for(String name: blackListNames){
                            blackListTuple.add(new Tuple2<String, Boolean>(name, true));
                        }

                        // 数据来自于查询的黑名单表并且映射成为<String, Boolean>
                        List<Tuple2<String, Boolean>> blackListFromDB = blackListTuple;
                        JavaSparkContext jsc = new JavaSparkContext(rdd.context());
                        /**
                         * 黑名单列表只有userID，但是如果要进行Join操作，就必须使用Key-Value，
                         * 所有这里我们需要机遇数据表中的数据产生Key-Value类型的数据集合
                         */
                        JavaPairRDD<String, Boolean> blackListRDD = jsc.parallelizePairs(blackListFromDB);

                        JavaPairRDD<String, Tuple2<String, String>> rdd2Pair = rdd.mapToPair(
                                new PairFunction<ConsumerRecord<String, String>, String, Tuple2<String, String>>() {
                                    @Override
                                    public Tuple2<String, Tuple2<String, String>> call(ConsumerRecord<String, String> t) throws Exception {
                                        String userID = t.value().split("\t")[2];
                                        return  new Tuple2<String, Tuple2<String, String>>(userID, new Tuple2<String, String>(t.key(), t.value()));
                                    }
                                }
                        );

//                        JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joined = rdd2Pair.leftOuterJoin(blackListRDD);

                        JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joined = rdd2Pair.leftOuterJoin(blackListRDD);

                        JavaPairRDD<String, String> result = joined.filter(
                                new Function<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                    @Override
                                    public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> v1) throws Exception {
                                        Optional<Boolean> optional = v1._2._2;

                                        if(optional.isPresent() && optional.get()){
                                            return false;
                                        }else {
                                            return true;
                                        }
                                    }
                                }
                        ).mapToPair(
                                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    @Override
                                    public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> t) throws Exception {
                                        return t._2._1;
                                    }
                                }
                        );

                        return result;
                    }
                }
        );

        filteredadClickedStreaming.print();


        /**
         * 第四步： 接下来就像对RDD编程一样，基于DStream进行编程，原因是DStream是RDD生产的模板（或者说类），
         * 在Spark Streaming具体发生计算前，其实质是把每个Batch的DStream操作翻译成对RDD的操作
         *
         * 广告点击的基本数据格式：timestamp、ip、userID、addID、province、city
         */
        JavaPairDStream<String, Long> pairs = filteredadClickedStreaming.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                        String[] splited = t._2.split("\t");

                        String timestamp = splited[0];      // yyyy-MM-dd
                        String ip = splited[1];
                        String userID = splited[2];
                        String adID = splited[3];
                        String province = splited[4];
                        String city = splited[5];

                        String clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_" + city;

                        return new Tuple2<String, Long>(clickedRecord , 1L);
                    }
                }
        );

        /**
         * 对初始的DStream进行 Transformation级别的处理，
         *
         * 计算每个Batch Duration中每个User的广告点击量
         */
        JavaPairDStream<String, Long> adClickedUsers = pairs.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        // 代办事项： 自动生成方法存根
                        return v1 + v2;
                    }
                }
        );


        /**
         * 有效点击：
         *      ① 对于复杂化的，一般都采用机器学习训练好模型直接在线进行过滤；
         *
         *      ② 对于简单的，可以通过一个BatchDuration中的点击次数判断是不是非法广告点击，但实际上，非法广告点击程序会尽可能模拟真实的广告点击行为，
         *      所以通过一个Batch来判断是不完整的，我们需要对例如一天（也可以是每个小时）的数据进行判断！
         *
         *      ③比在线机器学习退而求其次的做法如下：例如，一段时间内，同一IP（MAC地址）有多个用户的账号访问；
         *
         *      例如：可以统计一天内一个用户点击广告的次数，如果一天内点击同样的广告操作50次，就列入黑名单；
         *
         *
         * 黑名单有一个重要的特征：动态生产，所以，每个Batch Duration都要思考是否有新的黑名单加入，因此黑名单需要存储起来。
         *
         * 存储在什么地方：一般村粗在DB或Redis中即可
         *      例如有家中的"黑名单"，可以采用Spark Streaming不断地监控每个用户的操作，如果用户发送邮件的频率超过了设定的值，
         *      则可以暂时把用户列入"黑名单"，从而阻止用户过滤频繁的发送邮件。
         *
         */
        JavaPairDStream<String, Long> filteredClickInBatch = adClickedUsers.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> v1) throws Exception {
                        if(1 < v1._2){
                            // 更新黑名单的数据表
                            return false;
                        }else {
                            return true;
                        }
                    }
                }
        );


        /**
         * 需要注意，Spark Streaming应用程序想要执行具体的Job，对DStream就必须有output Stream操作，
         * output Stream有很多类型的函数触发，如：print、saveAsTextFile、saveAsHadoopFiles等，最重要的一个方法是foreachRDD、
         *
         * Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面。
         */
//        filteredClickInBatch.foreachRDD( new Function<JavaPairRDD<String, Long>, Void>(){
        filteredClickInBatch.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                if(rdd.isEmpty()){

                }

                rdd.foreachPartition(
                        new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                            @Override
                            public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                                /**
                                 * 这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库Mysql；
                                 *
                                 * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效地操作，我们需要批量处理。
                                 * 例如：一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作
                                 *
                                 * 插入的用户信息可以包含timestamp、ip、userID、adID、province、city
                                 *
                                 * 当出现两条记录的key是一样的情况，此时就需要更新累加操作
                                 *
                                 */

                                List<UserAdClicked> userAdClickedList = new ArrayList<>();
                                while (partition.hasNext()){
                                    Tuple2<String, Long> record = partition.next();
                                    String[] splited = record._1.split("_");
                                    UserAdClicked userAdClicked = new UserAdClicked();
                                    userAdClicked.setTimestamp(splited[0]);
                                    userAdClicked.setIp(splited[1]);
                                    userAdClicked.setUserID(splited[2]);
                                    userAdClicked.setAdID(splited[3]);
                                    userAdClicked.setProvince(splited[4]);
                                    userAdClicked.setCity(splited[5]);
                                }

                                final List<UserAdClicked> inserting = new ArrayList<>();
                                final List<UserAdClicked> updating = new ArrayList<>();

                                JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();

                                // 点击
                                for(final UserAdClicked clicked : userAdClickedList){
                                    jdbcWrapper.doQuery(
                                            "SELECT COUNT(1) FROM adclicked WHERE timestamp=? AND userID=? AND adID=?",
                                            new Object[]{
                                                    clicked.getTimestamp(),
                                                    clicked.getUserID(),
                                                    clicked.getAdID()
                                            },
                                            new ExecuteCallBack() {
                                                @Override
                                                public void resultCallBack(ResultSet result) throws Exception {
                                                    if(result.getRow() != 0){
                                                        long count = result.getLong(1);
                                                        clicked.setClickedCount(count);
                                                        updating.add(clicked);
                                                    }else{
                                                        clicked.setClickedCount(0L);
                                                        inserting.add(clicked);
                                                    }
                                                }
                                            }
                                    );
                                }

                                // 点击表的字段： timestamp、ip、userID、adID、province、city、cilckedCount
                                ArrayList<Object[]> insertParametersList = new ArrayList<>();
                                for(UserAdClicked inserRecord : inserting){
                                    insertParametersList.add(
                                            new Object[]{
                                                    inserRecord.getTimestamp(),
                                                    inserRecord.getIp(),
                                                    inserRecord.getUserID(),
                                                    inserRecord.getAdID(),
                                                    inserRecord.getProvince(),
                                                    inserRecord.getCity(),
                                                    inserRecord.getClickedCount()
                                            }
                                    );
                                    jdbcWrapper.doBatch("INSERT INTO adclicked VALUES(?,?,?,?,?,?,?)", insertParametersList);

                                }

                                //
                                ArrayList<Object[]> updateParametersList = new ArrayList<>();
                                for(UserAdClicked updateRecord : updating){
                                    updateParametersList.add(new Object[]{
                                            updateRecord.getTimestamp(),
                                            updateRecord.getIp(),
                                            updateRecord.getUserID(),
                                            updateRecord.getAdID(),
                                            updateRecord.getProvince(),
                                            updateRecord.getCity(),
                                            updateRecord.getClickedCount()
                                    });
                                }
                                jdbcWrapper.doBatch(
                                        "UPDATE adclicked SET clickedCount=? WHERE timestamp=? AND ip=? AND userID=? AND adID=? AND province=? AND city=? ",
                                        updateParametersList
                                );
                            }
                        }
                );
                return;

            }
        });



        JavaPairDStream<String, Long> blackListBasedOnHistory = filteredClickInBatch.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> v1) throws Exception {
                        // timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_" + city;
                        String[] splited = v1._1.split("_");
                        String date = splited[0];
                        String userID = splited[2];
                        String adID = splited[3];

                        /**
                         * 接下来根据date、userID、adID等条件查询用户点击广告的数据表，获得总的点击次数
                         * 根据这个点击次数判断是否属于黑名单，假设查出的数据是81
                         */
                        int clickedCounttotalToday = 81;

                        if(clickedCounttotalToday > 50){
                            return true;
                        }else{
                            return false;
                        }
                    }
                }
        );



        /**
         * 对黑名单整个RDD进行去重操作
         */
        JavaDStream<String> blackListuserIDtBasedOnHistory = blackListBasedOnHistory.map(
                new Function<Tuple2<String, Long>, String>() {
                    @Override
                    public String call(Tuple2<String, Long> v1) throws Exception {
                        return v1._1.split("_")[2];
                    }
                }
        );

        JavaDStream<String> blackListUniqueuserIDtBasedOnHistory = blackListuserIDtBasedOnHistory.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        return rdd.distinct();
                    }
                }
        );



        /**
         * 写入黑名单数据表中
         */
        blackListUniqueuserIDtBasedOnHistory.foreachRDD(
                new VoidFunction<JavaRDD<String>>() {
                    @Override
                    public void call(JavaRDD<String> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<String>>() {
                                    @Override
                                    public void call(Iterator<String> t) throws Exception {
                                        List<Object[]> blackList = new ArrayList<>();
                                        while (t.hasNext()){
                                            blackList.add(new Object[]{(Object)t.next()});
                                        }
                                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                                        jdbcWrapper.doBatch("INSERT INTO blacklisttable VALUES(?)", blackList);
                                    }
                                }
                        );
                        return;
                    }
                }
        );


        /**
         * 广告点击累计动态更新，每个updateStateByKey都会在BatchDuration的时间间隔的基础上进行更高点击次数的更新，
         * 更新之后，我们一般都会持久化到外部存储设备上，比如Mysql
         *
         */
        JavaPairDStream<String, Long> updateStateByKeyDStream = filteredadClickedStreaming.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                        String[] splited = t._2.split("\t");
                        String timestamp = splited[0];      // yyyy-MM-dd
                        String ip = splited[1];
                        String userID = splited[2];
                        String adID = splited[3];
                        String province = splited[4];
                        String city = splited[5];

                        String clickedRecord = timestamp + "_"  + adID + "_" + province + "_" + city;

                        return new Tuple2<String, Long>(clickedRecord, 1L);
                    }
                }
        ).updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    /**
                     * v1: 代表当前的key在当前的Batch Duration中出现次数的集合，如{1,1,1,1,1,1}
                     * v2: 代表当前key在一起拿的BatchDuration中积累下来的结果
                     */
                    @Override
                    public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
                        Long clickedTotalHistory = 0L;
                        if(v2.isPresent()){
                            clickedTotalHistory = v2.get();
                        }
                        for(Long one : v1){
                            clickedTotalHistory += one;
                        }

                        return Optional.of(clickedTotalHistory);
                    }
                }
        );



        updateStateByKeyDStream.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                                        List<AdClicked> adClickedList = new ArrayList<>();

                                        while (partition.hasNext()){
                                            Tuple2<String, Long> record = partition.next();
                                            String[] splited = record._1.split("_");
                                            AdClicked adClicked = new AdClicked();

                                            adClicked.setTimestamp(splited[0]);
                                            adClicked.setAdID(splited[1]);
                                            adClicked.setProvince(splited[2]);
                                            adClicked.setCity(splited[3]);
                                            adClicked.setClickedCount(record._2);

                                            adClickedList.add(adClicked);
                                        }
                                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                                        final List<AdClicked> inserting = new ArrayList<>();
                                        final List<AdClicked> updating = new ArrayList<>();

                                        for(final AdClicked clicked : adClickedList){
                                            jdbcWrapper.doQuery(
                                                    "SELECT COUNT(1) FROM adclickedcount WHERE timestamp=? AND adID=? AND province=? AND city=?",
                                                    new Object[]{
                                                            clicked.getTimestamp(),
                                                            clicked.getAdID(),
                                                            clicked.getProvince(),
                                                            clicked.getCity()
                                                    },
                                                    new ExecuteCallBack() {
                                                        @Override
                                                        public void resultCallBack(ResultSet result) throws Exception {
                                                            if(result.getRow() != 0){
                                                                long count = result.getLong(1);
                                                                clicked.setClickedCount(count);
                                                                updating.add(clicked);
                                                            }else{
                                                                inserting.add(clicked);
                                                            }
                                                        }
                                                    }
                                            );
                                        }

                                        ArrayList<Object[]> insertParametersList = new ArrayList<>();
                                        for(AdClicked inserRecord : inserting){
                                            insertParametersList.add(
                                                    new Object[]{
                                                            inserRecord.getTimestamp(),
                                                            inserRecord.getAdID(),
                                                            inserRecord.getProvince(),
                                                            inserRecord.getCity(),
                                                            inserRecord.getClickedCount()
                                                    }
                                            );
                                            jdbcWrapper.doBatch("INSERT INTO adclickedcount VALUES(?,?,?,?,?)", insertParametersList);
                                        }

                                        ArrayList<Object[]> updateParametersList = new ArrayList<>();
                                        for(AdClicked updateRecord : updating){
                                            updateParametersList.add(
                                                    new Object[]{
                                                            updateRecord.getClickedCount(),
                                                            updateRecord.getTimestamp(),
                                                            updateRecord.getAdID(),
                                                            updateRecord.getProvince(),
                                                            updateRecord.getCity()
                                                    }
                                            );
                                            jdbcWrapper.doBatch(
                                                    "UPDATE adclickedcount set clickedCount=? WHERE timestamp=? AND adID=? AND province=? AND city=?",
                                                    updateParametersList
                                            );
                                        }
                                    }

                                }
                        );
                        return;
                    }
                }
        );



        /**
         * 对广告点击进行TopN的计算，计算出每天每个省份的Top5排名的广告；
         * 因为我们直接对RDD进行操作，所以使用transform算子
         */
        updateStateByKeyDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                        JavaRDD<Row> rowRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, Long>, String, Long>() {
                                    @Override
                                    public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
                                        String[] splited = t._1.split("_");
                                        String timestamp = "2018-03-08";
                                        String adID = splited[1];
                                        String province = splited[2];
                                        String clickedRecord = timestamp + "_" + adID + "_" + province;
                                        return new Tuple2<String, Long>(clickedRecord, t._2);
                                    }
                                }
                        ).reduceByKey(
                                new Function2<Long, Long, Long>() {
                                    @Override
                                    public Long call(Long v1, Long v2) throws Exception {
                                        return v1 + v2;
                                    }
                                }
                        ).map(
                                new Function<Tuple2<String, Long>, Row>() {
                                    @Override
                                    public Row call(Tuple2<String, Long> v1) throws Exception {
                                        String[] splited = v1._1.split("_");
                                        String timestamp = "2018-03-08";
                                        String adID = splited[1];
                                        String province = splited[2];
                                        return RowFactory.create(timestamp, adID, province, v1._2);
                                    }
                                }
                        );

                        StructType structType = DataTypes.createStructType(
                                Arrays.asList(
                                        DataTypes.createStructField("timestamp", DataTypes.StringType, true),
                                        DataTypes.createStructField("adID", DataTypes.StringType, true),
                                        DataTypes.createStructField("province", DataTypes.StringType, true),
                                        DataTypes.createStructField("clickedCount", DataTypes.LongType, true)

                                )
                        );
//                        HiveContext hiveContext = new HiveContext(rdd.context());
//                        DataFrame df = hiveContext.createDataFrame(rowRDD, structType);
                        SparkSession ss = SparkSession.builder()
                                .config(rdd.context().conf())
                                .enableHiveSupport().getOrCreate();
                        Dataset<Row> df = ss.createDataFrame(rowRDD, structType);
                        df.registerTempTable("topNTableSource");

                        String IMFsqlText = "SELECT timestamp,adID,province,clickedCount FROM (SELECT timestamp,adID,province,clickedCount,row_number() " +
                                "OVER(PARTITION BY province ORDER BY clickedCount DESC) rank FROM topNTableSource) subquery WHERE rank<=5";
                        Dataset<Row> result = ss.sql(IMFsqlText);

                        return result.toJavaRDD();
                    }
                }
        ).foreachRDD(
                new VoidFunction<JavaRDD<Row>>() {
                    @Override
                    public void call(JavaRDD<Row> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Row>>() {
                                    @Override
                                    public void call(Iterator<Row> t) throws Exception {
                                        List<AdProvinceTopN> adProvinceTopNS = new ArrayList<>();

                                        while (t.hasNext()){
                                            Row row = t.next();

                                            AdProvinceTopN item = new AdProvinceTopN();
                                            item.setTimestamp(row.getString(0));
                                            item.setAdID(row.getString(1));
                                            item.setProvince(row.getString(2));
                                            item.setClickedCount(row.getLong(3));
                                            adProvinceTopNS.add(item);
                                        }
                                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();

                                        Set<String> set = new HashSet<>();
                                        for(AdProvinceTopN item : adProvinceTopNS){
                                            set.add(item.getTimestamp() + " " + item.getProvince());
                                        }

                                        ArrayList<Object[]> deleteParametersList = new ArrayList<>();
                                        for(String deleteRecord : set){
                                            String[] splited = deleteRecord.split("_");
                                            deleteParametersList.add(new Object[]{
                                                splited[0], splited[1]
                                            });
                                        }
                                        jdbcWrapper.doBatch("DELETE FROM adprovincetopn WHERE timestamp=? AND province=?", deleteParametersList);

                                        // 广告点击每个省份的TopN排名
                                        ArrayList<Object[]> insertParametersList = new ArrayList<>();
                                        for(AdProvinceTopN updateRecored : adProvinceTopNS){
                                            insertParametersList.add(new Object[]{
                                                    updateRecored.getTimestamp(),
                                                    updateRecored.getAdID(),
                                                    updateRecored.getProvince(),
                                                    updateRecored.getClickedCount()
                                            });
                                        }
                                        jdbcWrapper.doBatch("INSERT INTO adprovincetopn VALUES(?,?,?,?)", insertParametersList);
                                    }
                                }
                        );
                        return;
                    }
                }
        );



        /**
         * 计算过去半小时内广告点击的趋势，
         * 用户广告点击信息可以包含timestamp、ip、userID、adID、province、city
         */
        filteredadClickedStreaming.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                        String[] splited = t._2.split("\t");
                        String adID = splited[3];
                        String time = splited[0];
                        return new Tuple2<String, Long>(time + "_" + adID, 1L);
                    }
                }
        ).reduceByKeyAndWindow(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v2 - v1;
                    }
                }, new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 - v2;
                    }
                },
                Durations.minutes(30),
                Durations.milliseconds(1)
        ).foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                                        List<AdTrendStat> adTrend = new ArrayList<>();
                                        while (partition.hasNext()){
                                            Tuple2<String, Long> record = partition.next();
                                            String[] splited = record._1.split("_");
                                            String time = splited[0];
                                            String adID = splited[1];
                                            Long clickedCount = record._2;

                                            /**
                                             * 我们通过J2EE技术进行趋势绘图的时候肯定是需要年、月、日、分这些维度的，所以我们在这里要将这些维度保存
                                             */
                                            AdTrendStat adTrendStat = new AdTrendStat();
                                            adTrendStat.setAdID(adID);
                                            adTrendStat.setClickedCount(clickedCount);
                                            //todo time中提取天时分
                                            adTrendStat.set_date(time);
                                            adTrendStat.set_hour(time);
                                            adTrendStat.set_minute(time);

                                            adTrend.add(adTrendStat);
                                        }

                                        final List<AdTrendStat> inserting = new ArrayList<>();
                                        final List<AdTrendStat> updating = new ArrayList<>();

                                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
                                        for(final AdTrendStat clicked : adTrend){
                                            final AdTrendCountHistory adTrendCountHistory = new AdTrendCountHistory();
                                            jdbcWrapper.doQuery(
                                                    "SELECT count(1) FROM adclickedtrend WHERE date=? AND hour=? AND minute=? AND adID=?",
                                                    new Object[]{clicked.get_date(), clicked.get_hour(), clicked.get_minute(), clicked.getAdID()},
                                                    new ExecuteCallBack() {
                                                        @Override
                                                        public void resultCallBack(ResultSet result) throws Exception {
                                                            if(result.getRow() != 0){
                                                                long count = result.getLong(1);
                                                                adTrendCountHistory.setClickedCountHistory(count);
                                                                updating.add(clicked);
                                                            }else{
                                                                inserting.add(clicked);
                                                            }
                                                        }
                                                    }
                                            );
                                        }

                                        //广告点击趋势
                                        ArrayList<Object[]> insertParametersList = new ArrayList<>();
                                        for(AdTrendStat inserRecord : inserting){
                                            insertParametersList.add(new Object[]{
                                                    inserRecord.get_date(),
                                                    inserRecord.get_hour(),
                                                    inserRecord.get_minute(),
                                                    inserRecord.getAdID(),
                                                    inserRecord.getClickedCount()
                                            });
                                        }
                                        jdbcWrapper.doBatch("INSERT INTO adclickedtrend VALUES(?,?,?,?,?)", insertParametersList);

                                        ArrayList<Object[]> updateParametersList = new ArrayList<>();
                                        for(AdTrendStat updateRecord : updating){
                                            updateParametersList.add(new Object[]{
                                                    updateRecord.getClickedCount(),
                                                    updateRecord.get_date(),
                                                    updateRecord.get_hour(),
                                                    updateRecord.get_minute(),
                                                    updateRecord.getAdID()
                                            });
                                        }
                                        jdbcWrapper.doBatch("UPDATE adclickedtrend set clickedCount=? WHERE date=? AND hour=? AND minute=? AND adID=?", updateParametersList);

                                    }
                                }
                        );
                        return;
                    }
                }
        );


        /**
         * Spark Streaming 执行引擎也就是Driver 开始运行，Driver启动时是位于一条新的线程中能够的，
         * 当然其内部有消息循环体，用于接收应用程序本身或者Executor的消息
         */
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }

}

