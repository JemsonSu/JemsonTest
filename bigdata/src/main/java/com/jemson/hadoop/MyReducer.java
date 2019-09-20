package com.jemson.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /*
	 <a,1>,<a,1>,<a,1>
	 <b,1>,<b,1>,<b,1>
	 <d,1>,<d,1>,<d,1>
	 map结果集的每一组数据调用一次reduce方法
	 */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;//计算每组数据每个单词个数汇总

        for(IntWritable value : values){
            sum += value.get();
        }

        //写出单词和汇总数
        context.write(key, new IntWritable(sum));



    }
}
