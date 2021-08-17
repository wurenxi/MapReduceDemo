package com.atguigu.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author gxl
 * @date 2021年08月17日 19:53
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //准备初始化集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        //循环遍历
        for (TableBean value : values) {
            if("order".equals(value.getFlag())){ //订单表

                TableBean tmpTableBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tmpTableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderBeans.add(tmpTableBean);
            }else{ //商品表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //循环遍历orderBeans，赋值pdname
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());

            context.write(orderBean,NullWritable.get());
        }

    }
}
