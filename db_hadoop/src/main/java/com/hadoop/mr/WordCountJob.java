package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 读取hdfs上的hello.txt文件，计算文件中每个单词出现的总次数
 * @author LR
 * @create 2021-05-08:20:41
 */
public class WordCountJob {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        /*
         map函数：接收<k1,v1>, 产生<k2,v2>
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            // k1 表示每一行数据的行首偏移量，v1表示每一行内容
            // 对每一行数据进行切割，把单词切割出来
            String[] words = v1.toString().split(" ");

            // 迭代切割出来的单词数据
            for (String word:words) {
                // 迭代切割出来的单词封装成<k2,v2>的形式
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                // 把<k2,v2>写出去
                context.write(k2, v2);
            }

        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        // 针对<k2,{v2...}>的数据进行累积求和，并且最终把数据转化为k3,v3写出去


        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {
            // 创建一个sum变量，保存v2s的和
            long sum = 0L;
            for(LongWritable v2 : v2s){
                sum += v2.get();
            }

            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            context.write(k3, v3);
        }
    }

    // 组装Job=Map+Reduce
    public static void main(String[] args) {

        try{
            if(args.length != 2){
                // 如果传入的参数不够，程序直接退出
                System.exit(100);
            }
            // Job需要的参数
            Configuration conf = new Configuration();
            // 创建一个Job
            Job job = Job.getInstance(conf);

            // 必须设置，否则在集群中执行的时候找不到WordCountJob这个类
            job.setJarByClass(WordCountJob.class);

            // 指定输入路径(可以是文件，也可以是目录)
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            // 指定输出目录（该目录必须不存在）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 指定map相关代码
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // 指定reducer相关代码
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // 提交Job
            job.waitForCompletion(true);
        }catch (Exception e){
            e.printStackTrace();
        }


    }



}
