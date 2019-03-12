package yore.dt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模拟数据生成
 *
 * kafka-topics.sh --create --zookeeper cdh2:2181,cdh3:2181,cdh4:2181,cdh5:2181,cdh6:2181 --partitions 2 --replication-factor 1 --topic AdClicked
 *
 * kafka-console-consumer.sh --bootstrap-server cdh3:9092,cdh4:9092,cdh5:9092 --from-beginning --topic AdClicked
 *
 * Created by yore on 2019/3/8 15:33
 */
public class MockAdClickedStats {

    public static final String KAFKA_TOPIC = "AdClicked";

    public static void main(String[] args) {
        final Random random = new Random();
        final String[] provinces = new String[]{
                "Guangdong", "Zhejiang","Jiangsu", "Fujian",
                "Beijing", "Henan", "Gansu", "Liaoning"
        };
        final Map<String, String[]> cities = new HashMap<String, String[]>(){{
            put("Guangdong", new String[]{"Guangzhou", "Shenzhen", "DongGuan"});
            put("Zhejiang", new String[]{"Hangzhou", "Wenzhou", "Ningbo"});
            put("Jiangsu", new String[]{"Nanjing", "Suzhou", "Wuxi"});
            put("Fujian", new String[]{"Fuzhou", "Xiamen", "Sanming"});
            put("Beijing", new String[]{"Beijing"});put("Henan", new String[]{"Zhengzhou"});
            put("Gansu", new String[]{"Lanzhou"});put("Liaoning", new String[]{"Shenyang"});
        }};
        final String[] ips = new String[]{
                "192.168.112.240",  "192.168.112.239",  "192.168.112.245",  "192.168.112.246",  "192.168.112.247",
                "192.168.112.248",  "192.168.112.249",  "192.168.112.250",  "192.168.112.251",  "192.168.112.252",
                "192.168.112.253","192.168.112.254"
        };

        /**
         * Kafka配置的基本信息
         */
//        Properties kafkaConf = new Properties();
//        kafkaConf.put("serializer.class", "kafka.serializer.StringEncoder");
//        kafkaConf.put("matadata.broker.list", "cdh3:9092,cdh4:9092,cdh5:9092");
//        ProducerConfig producerConfig = new ProducerConfig(kafkaConf);
//        final Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);

        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh3:9092,cdh4:9092,cdh5:9092");
        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final Producer<String, String> producer = new KafkaProducer<>(props);

        final int index = 0;
        final AtomicInteger ai=new AtomicInteger(0);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    // 在线处理广告点击流，广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
                    Long timestamp = new Date().getTime();
                    String ip = ips[random.nextInt(12)];
                    int userID = random.nextInt(10000);
                    int adID = random.nextInt(100);
                    String province = provinces[random.nextInt(8)];
                    String city = cities.get(province)[random.nextInt(cities.get(province).length)];

                    String clickedAd = timestamp + "\t" + ip + "\t" + userID + "\t" + adID + "\t" + province + "\t" + city ;
                    System.out.println(ai.incrementAndGet() + ":\t\t" + clickedAd);


                    producer.send(new ProducerRecord<String, String>(KAFKA_TOPIC, clickedAd));

                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                }

            }
        }).start();



    }

}
