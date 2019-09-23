package com.jemson.spark2;

import org.apache.commons.lang3.StringUtils;
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
 * 使用jdk8 Lamb表达式
 */
public class LambSparkWC {
    public static void main(String[] args) {



        System.out.println("spark2.4.4 wordcount 使用Lamb表达式");

        String logFile = "./bigdata/a.txt";
        //String logFile = args[0];
        System.out.println("计算文件:"+logFile);

        SparkSession sparkSession = SparkSession.builder()
                .appName("LambSparkWC")
                .master("local")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR"); //只输出ERROR级别日志

        JavaRDD<String> lines = sparkSession.read().textFile(logFile).javaRDD();
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(StringUtils.split(line)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((v1, v2) -> v1 + v2);

        counts.foreach(t -> System.out.println(t._1 + "==" + t._2()));



        //关闭资源，close方法也是调用stop方法
        sparkSession.stop();

    }

}
