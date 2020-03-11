package com.wanglikai.userdraw.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserDrawMapReduceStepTwoPartitioner extends Partitioner<StepTwoMapBean, NullWritable> {
    @Override
    public int getPartition(StepTwoMapBean stepTwoMapBean, NullWritable nullWritable, int numPartitions) {
        return (stepTwoMapBean.getMdn().hashCode() & Integer.MAX_VALUE)%numPartitions;
    }
}
