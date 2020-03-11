package com.wanglikai.userdraw.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class UserDrawPutHBaseMapReduce {

    public static class UserDrawPutHBaseMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    public static class UserDrawPutHBaseReducer extends TableReducer<Text, NullWritable,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for(NullWritable value :values){
                String[] arr = key.toString().split("[|]");
                if(arr.length==8 && (!StringUtils.isEmpty(arr[0]))){
                    String rowKey = arr[0];
                    Put put = new Put(Bytes.toBytes(rowKey));

                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("mdn"),Bytes.toBytes(arr[0]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("male"),Bytes.toBytes(arr[1]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("female"),Bytes.toBytes(arr[2]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age1"),Bytes.toBytes(arr[3]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age2"),Bytes.toBytes(arr[4]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age3"),Bytes.toBytes(arr[5]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age4"),Bytes.toBytes(arr[6]));
                    put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age5"),Bytes.toBytes(arr[7]));

                    context.write(NullWritable.get(),put);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.addResource("hbase-site.xml");

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserDrawPutHBaseMapReduce.class);
        job.setJobName("ToHBase");

        //初始化TableReducer，设置hbase表名
        TableMapReduceUtil.initTableReducerJob("t_draw", UserDrawPutHBaseReducer.class,job);
        job.setMapperClass(UserDrawPutHBaseMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path("C:\\sparkdata\\userDraw\\output2"));

        job.waitForCompletion(true);
    }

}
