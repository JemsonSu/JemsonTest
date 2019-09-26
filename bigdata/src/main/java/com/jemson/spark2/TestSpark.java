package com.jemson.spark2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

public class TestSpark {
    public static void main(String[] args) {

        test3();






    }

    public static void test3() {
        String projectDir = System.getProperty("user.dir");  //项目目录
        System.out.println(projectDir);

        String logFile = projectDir + "/files/persistData.txt";
        System.out.println("logFile=" + logFile);

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("TestSpark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("error");
        JavaRDD<String> lines = jsc.textFile(logFile);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((v1, v2) -> v1 + v2);
        counts.foreach(t -> System.out.println(t._1+"::"+t._2()));


        jsc.stop();


    }

    public static void test2(){
        String projectDir = System.getProperty("user.dir");  //项目目录
        System.out.println(projectDir);

        String logFile = projectDir + "/files/persistData.txt";
        System.out.println("logFile="+logFile);

        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("TestSpark")
                .getOrCreate();
        DataFrameReader read = session.read();
        Dataset<String> ds = read.textFile(logFile);
        JavaRDD<String> lines = ds.javaRDD();
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] arr = line.split("\t");
                List<String> list = Arrays.asList(arr);
                return list.iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });



    }


    public static void test1(){
        String projectDir = System.getProperty("user.dir");  //项目目录
        System.out.println(projectDir);

        String logFile = projectDir + "/files/persistData.txt";
        System.out.println("logFile="+logFile);

        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("TestSpark")
                .getOrCreate();
        session.sparkContext().setLogLevel("error");
        DataFrameReader read = session.read();

        Dataset<String> ds = read.textFile(logFile);
        JavaRDD<String> lines = ds.javaRDD();
        JavaRDD<String> works = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator());
        JavaPairRDD<String, Integer> ones = works.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((v1, v2) -> v1 + v2);
        counts.foreach(t -> System.out.println(t._1+"="+t._2()));
    }
}
