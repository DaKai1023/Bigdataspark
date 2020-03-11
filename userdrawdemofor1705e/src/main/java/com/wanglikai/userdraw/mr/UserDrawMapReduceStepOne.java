package com.wanglikai.userdraw.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserDrawMapReduceStepOne {

    public static class UserDrawStepOneMapper extends Mapper<LongWritable, Text,Text,Text> {

        private Text k;
        private Text v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("[|]");
            String uiqKey = arr[0] + arr[15]; //MDN（用户唯一标识）+ 用户使用的APPID
            String useDuration = arr[12]; //使用时长

            k.set(uiqKey);
            //    mdn+appid   使用时长        mdn        appid
            v.set(uiqKey+"|"+useDuration+"|"+arr[0]+"|"+arr[15]);

            context.write(k,v);
        }
    }

    //统计用户单个app的使用时长和使用次数
    public static class UserDrawStepOneReducer extends Reducer<Text,Text,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Long sum = 0L;
            Integer count = 0;
            String mdn = "";
            String appId = "";
            for(Text value : values) {
                String[] arr = value.toString().split("[|]");
                if(arr.length == 4) {
                    Long time = Long.valueOf(arr[1]);
                    sum = sum + time;
                    count += 1;
                    mdn = arr[2];
                    appId = arr[3];
                }
            }
            String uiqKey = key.toString();
            //                     uiqKey       用户唯一标识   appid         总使用时长    使用了几次
            Text k = new Text(uiqKey + "|" + mdn + "|" + appId + "|" + sum + "|" + count);
            context.write(k,NullWritable.get());
        }
    }


}
