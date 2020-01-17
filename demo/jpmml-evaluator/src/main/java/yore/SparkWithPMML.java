package yore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.jpmml.evaluator.DefaultVisitorBattery;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorBuilder;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.jpmml.evaluator.spark.TransformerBuilder;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * jpmml-evaluator-spark 的使用说明可以查看 <a href='https://github.com/jpmml/jpmml-evaluator-spark'>jpmml/jpmml-evaluator-spark</a>
 *
 * Created by yore
 */
public class SparkWithPMML {

    public static void main(String[] args) throws IOException, JAXBException, SAXException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("spark_pmml")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        if(args.length!=3){
            System.err.println("Usage: scala " + SparkWithPMML.class.getName() + " <PMML file path> <Input filePath> <Output directory>");
            System.exit(-1);
        }

        String pmmlPath = args[0];
        String inputFilePath = args[1];
        String outputPath = args[2];

        Dataset ds = sparkSession.read().option("header", "true")
                .option("inferSchema", "true")
                .csv(inputFilePath);
        ds.cache();
        ds.show();


        /*
         * 根据pmml文件，使用sparkmllib构建模型
         */
        FileSystem fs = FileSystem.get(new Configuration());
        InputStream pmmlIs = fs.open(new Path(pmmlPath));
        EvaluatorBuilder evaluatorBuilder = new LoadingModelEvaluatorBuilder()
                .setLocatable(false)
                .setVisitors(new DefaultVisitorBattery())
                .load(pmmlIs);

        Evaluator evaluator = evaluatorBuilder.build();


        TransformerBuilder modelBuilder = new TransformerBuilder(evaluator)
                .withTargetCols()
                .withOutputCols()
                .exploded(true);

        Transformer transformer = modelBuilder.build();

        /*
         * 预测
         */
        Dataset resultDS = transformer.transform(ds);
        resultDS.persist();

        resultDS.printSchema();
        resultDS.write().csv(outputPath);
        System.out.println(resultDS.rdd().getNumPartitions());

    }
}
