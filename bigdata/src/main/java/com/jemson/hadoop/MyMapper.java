package com.jemson.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * mapreduce 的 map阶段
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString(); //每调用一次map方法，拿到一行数据
        String[] words = line.split(" ", -1);

        for (String word : words) {
            //用单词做key,value是单词计数(+1)
            context.write(new Text(word), new IntWritable(1));
        }


    }
}
