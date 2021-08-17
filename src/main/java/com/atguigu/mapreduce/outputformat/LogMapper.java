package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author gxl
 * @date 2021年08月16日 17:27
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //不做任何处理
        context.write(value,NullWritable.get());
    }
}
