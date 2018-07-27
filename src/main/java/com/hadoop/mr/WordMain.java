package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author xghuang
 * @date 2018/7/27
 * @time 11:01
 * @desc:
 */
public class WordMain {
    public static void main(String[] args) throws Exception {
        // Configuration 类：读取Hadoop 的配置文件，如 site-core.xml􀄀；
        // 也可用set 方法重新设置（会覆􁄥）：conf.set("fs.default.name","hdfs://xxxx:9000")
        Configuration conf = new Configuration();
        //将命令行中参数自动设置到变量conf 中
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2)
        {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        // 新建一个 job，传入配置信息
        Job job = new Job(conf, "word count");
        // 设置主类
        job.setJarByClass(WordMain.class);
        // 设置Mapper 类
        job.setMapperClass(WordMapper.class);
        // 设置作业合成类
        job.setCombinerClass(WordReducer.class);
        // 设置Reducer 类
        job.setReducerClass(WordReducer.class);
        // 设置输出数据的关键类
        job.setOutputKeyClass(Text.class);
        // 设置输出类
        job.setOutputValueClass(IntWritable.class);
        // 文件输入
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 文件输出
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        // 等待完成输出
        System.exit(job.waitForCompletion(true) ? 0 : 1 );
    }
}
