package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gxl
 * @date 2021年08月16日 17:30
 */
public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //防止有相同数据，丢失数据
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
