package com.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author xghuang
 * @date 2018/7/27
 * @time 10:17
 * @desc:
 */
public class WordMapper extends Mapper<Object,Text,Text,IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     *
     * @param key 首字母偏移量
     * @param value 文件一行内容
     * @param context Mapper 端的上下文，与 OutputCollector 和Reporter 的功能类􀍪
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while(stringTokenizer.hasMoreTokens()) {
            word.set(stringTokenizer.nextToken());
            context.write(word,one);
        }
    }
}
