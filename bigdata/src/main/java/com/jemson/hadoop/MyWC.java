package com.jemson.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 这是一个hadoop wordcout程序
 */
public class MyWC {
    public static void main(String[] args) throws Exception {
        System.out.println("");



        Configuration conf = new Configuration();
        //跑本地时设置下面两行
        //conf.set("fs.defaultFS", "file:///"); //打包到服务器跑注掉这个
        //System.setProperty("hadoop.home.dir", "D:\\hadoop-3.2.0");

        Job job = Job.getInstance(conf, "word count 2");
        job.setJarByClass(MyWC.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(X.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //linux
        FileInputFormat.addInputPath(job, new Path("/user/root/testwc.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/out2"));

        //windows
        //FileInputFormat.addInputPath(job, new Path("file:///f:/test/a.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("file:///f:/test/out3001"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println("完毕！");


    }
}
