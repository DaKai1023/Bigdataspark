package com.wanglikai.userdraw.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UserDrawReadHBaseMapReduce {

    public static class UserDrawReadHBaseMapper extends TableMapper<Text, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            byte[] mdnByte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mdn"));

            if(mdnByte != null && mdnByte.length != 0) {
                sb.append(Bytes.toString(mdnByte) + "|");
            }
            byte[] maleByte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("male"));
            if(maleByte != null && maleByte.length != 0) {
                sb.append(Bytes.toString(maleByte) + "|");
            }
            byte[] femaleByte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("female"));
            if(femaleByte != null && femaleByte.length != 0) {
                sb.append(Bytes.toString(femaleByte) + "|");
            }
            byte[] age1Byte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age1"));
            if(age1Byte != null && age1Byte.length != 0) {
                sb.append(Bytes.toString(age1Byte) + "|");
            }
            byte[] age2Byte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age2"));
            if(age2Byte != null && age2Byte.length != 0) {
                sb.append(Bytes.toString(age2Byte) + "|");
            }
            byte[] age3Byte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age3"));
            if(age3Byte != null && age3Byte.length != 0) {
                sb.append(Bytes.toString(age3Byte) + "|");
            }
            byte[] age4Byte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age4"));
            if(age4Byte != null && age4Byte.length != 0) {
                sb.append(Bytes.toString(age4Byte) + "|");
            }
            byte[] age5Byte = value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age5"));
            if(age5Byte != null && age5Byte.length != 0) {
                sb.append(Bytes.toString(age5Byte) + "|");
            }

            String str = sb.toString();
            String res = str.substring(0, str.length() - 1);

            context.write(new Text(res), NullWritable.get());
        }
    }

    public static class UserDrawReadHBaseReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for(NullWritable value : values) {
                context.write(key, NullWritable.get());
            }
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            conf.addResource("hbase-site.xml");

            Job job = Job.getInstance(conf);
            job.setJarByClass(UserDrawReadHBaseMapReduce.class);
            job.setJobName("ReadHBase");

            //初始化读取HBase表
            TableMapReduceUtil.initTableMapperJob(
                    "t_draw",new Scan()
                    ,UserDrawReadHBaseMapper.class
                    ,Text.class
                    ,NullWritable.class
                    ,job
                    ,false);

            job.setReducerClass(UserDrawReadHBaseReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileOutputFormat.setOutputPath(job, new Path("C:\\sparkdata\\userDraw\\fromhbase"));

            job.waitForCompletion(true);

        }
    }

}
