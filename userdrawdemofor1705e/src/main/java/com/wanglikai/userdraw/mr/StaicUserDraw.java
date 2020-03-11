package com.wanglikai.userdraw.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StaicUserDraw {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(StaicUserDraw.class);
        job.setJobName("UserDrawMapReduceStepOne");

        job.setMapperClass(UserDrawMapReduceStepOne.UserDrawStepOneMapper.class);
        job.setReducerClass(UserDrawMapReduceStepOne.UserDrawStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("C:\\sparkdata\\userDraw\\input"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\sparkdata\\userDraw\\output1"));

        if(job.waitForCompletion(true)) {
            conf.addResource("hbase-site.xml");
            Job job1 = Job.getInstance(conf);
            job1.setJarByClass(StaicUserDraw.class);
            job1.setJobName("ReadHBase");
            //初始化读取HBase表
            TableMapReduceUtil.initTableMapperJob(
                    "t_draw",new Scan()
                    , UserDrawReadHBaseMapReduce.UserDrawReadHBaseMapper.class
                    ,Text.class
                    ,NullWritable.class
                    ,job1
                    ,false);

            job1.setReducerClass(UserDrawReadHBaseMapReduce.UserDrawReadHBaseReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(NullWritable.class);

            FileOutputFormat.setOutputPath(job1, new Path("C:\\sparkdata\\userDraw\\fromhbase"));

            if(job1.waitForCompletion(true)) {
                Job job2 = Job.getInstance(conf);
                job2.setJarByClass(StaicUserDraw.class);
                job2.setJobName("UserDrawMapReduceStepTwo");

                job2.setMapperClass(UserDrawMapReduceStepTwo.UserDrawStepTwoMapper.class);
                job2.setReducerClass(UserDrawMapReduceStepTwo.UserDrawStepTwoReducer.class);

                job2.setMapOutputKeyClass(StepTwoMapBean.class);
                job2.setMapOutputValueClass(NullWritable.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);

                job2.setPartitionerClass(UserDrawMapReduceStepTwoPartitioner.class);
                job2.setGroupingComparatorClass(UserDrawMapReduceStepTwoGroupingCompartor.class);

                FileInputFormat.addInputPath(job2, new Path("C:\\sparkdata\\userDraw\\output1"));
                FileInputFormat.addInputPath(job2, new Path("C:\\sparkdata\\userDraw\\fromhbase"));
                FileOutputFormat.setOutputPath(job2, new Path("C:\\sparkdata\\userDraw\\output2"));

                //把计算好的数据从新覆盖hbase中的数据
                if(job2.waitForCompletion(true)) {
                    Job job3 = Job.getInstance(conf);
                    job3.setJarByClass(StaicUserDraw.class);
                    job3.setJobName("PutDataToHBase");

                    job3.setMapperClass(UserDrawPutHBaseMapReduce.UserDrawPutHBaseMapper.class);
                    TableMapReduceUtil.initTableReducerJob("t_draw", UserDrawPutHBaseMapReduce.UserDrawPutHBaseReducer.class,job3);

                    job3.setMapOutputKeyClass(Text.class);
                    job3.setMapOutputValueClass(NullWritable.class);
                    job3.setOutputKeyClass(NullWritable.class);
                    job3.setOutputValueClass(Put.class);

                    FileInputFormat.addInputPath(job3, new Path("C:\\sparkdata\\userDraw\\output2"));
                    job3.waitForCompletion(true);
                }
            }

        }
    }
}
