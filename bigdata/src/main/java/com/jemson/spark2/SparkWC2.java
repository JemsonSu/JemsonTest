package com.jemson.spark2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * spark 2.X 的wordcount 程序
 */
public class SparkWC2 {
    public static void main(String[] args) {
        System.out.println("spark2.4.4 wordcount 测试");

        //String logFile = "./bigdata/a.txt";
        String logFile = args[0];
        System.out.println("计算文件:"+logFile);

        SparkSession.Builder builder = SparkSession.builder();
        //builder.appName("spark2.4.4 wordcount");
        //builder.master("yarn");
        SparkSession sparkSession = builder.getOrCreate();
        //sparkSession.sparkContext().setLogLevel("ERROR"); //只输出ERROR级别日志

        DataFrameReader read = sparkSession.read();

        Dataset<String> dataset = read.textFile(logFile);


        JavaRDD<String> linesRDD = dataset.javaRDD(); //Dataset转javaRDD

        JavaRDD<String> rdd2 = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                System.out.println("line=" + line);
                String[] words = line.split(" ");
                List<String> list = Arrays.asList(words);
                return list.iterator();
            }
        });

        JavaPairRDD<String, Long> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String work) throws Exception {
                System.out.println("work:"+work);
                return new Tuple2<String, Long>(work, 1L);
            }
        });

        JavaPairRDD<String, Long> rdd4 = rdd3.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                System.out.println(v1+"--"+v2);
                return v1 + v2;
            }
        });


        rdd4.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> tuple2) throws Exception {
                System.out.println(tuple2._1 + "==" + tuple2._2());
            }
        });



        //关闭资源，close方法也是调用stop方法
        sparkSession.stop();

    }

}
