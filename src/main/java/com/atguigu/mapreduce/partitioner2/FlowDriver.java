package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author gxl
 * @date 2021年08月12日 8:46
 */
public class FlowDriver {

    public static void main(String[] args) throws Exception{

        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar
        job.setJarByClass(FlowDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6.设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output\\output5"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
