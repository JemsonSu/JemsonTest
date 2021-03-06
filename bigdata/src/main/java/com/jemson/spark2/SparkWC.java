package com.jemson.spark2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
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
public class SparkWC {
    public static void main(String[] args) {
        System.out.println("spark2.4.4 wordcount 测试");

        String logFile = "./bigdata/a.txt";
        //String logFile = args[0];
        System.out.println("计算文件:"+logFile);

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkWC");
        conf.setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> linesRDD = jsc.textFile(logFile);


        JavaRDD<String> worksRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                System.out.println("line=" + line);
                String[] words = line.split(" ");
                List<String> list = Arrays.asList(words);
                return list.iterator();
            }
        });

        JavaPairRDD<String, Long> onesRDD = worksRDD.mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String work) throws Exception {
                System.out.println("work:"+work);
                return new Tuple2<String, Long>(work, 1L);
            }
        });

        JavaPairRDD<String, Long> countsRDD = onesRDD.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                System.out.println(v1+"--"+v2);
                return v1 + v2;
            }
        });


        countsRDD.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> tuple2) throws Exception {
                System.out.println(tuple2._1 + "=" + tuple2._2());
            }
        });



        //关闭资源，close方法也是调用stop方法
        jsc.stop();

    }

}
