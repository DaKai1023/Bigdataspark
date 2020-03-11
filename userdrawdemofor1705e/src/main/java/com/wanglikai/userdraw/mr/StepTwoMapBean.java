package com.wanglikai.userdraw.mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StepTwoMapBean implements WritableComparable<StepTwoMapBean> {
//第一步的输出字段
    private String mdn;
    private String appId;
    private String times;
//原始画像的字段
    private String male;
    private String female;
    private String age1;
    private String age2;
    private String age3;
    private String age4;
    private String age5;

    private String status;


    public String getMdn() {
        return mdn;
    }

    public void setMdn(String mdn) {
        this.mdn = mdn;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getTimes() {
        return times;
    }

    public void setTimes(String times) {
        this.times = times;
    }

    public String getMale() {
        return male;
    }

    public void setMale(String male) {
        this.male = male;
    }

    public String getFemale() {
        return female;
    }

    public void setFemale(String female) {
        this.female = female;
    }

    public String getAge1() {
        return age1;
    }

    public void setAge1(String age1) {
        this.age1 = age1;
    }

    public String getAge2() {
        return age2;
    }

    public void setAge2(String age2) {
        this.age2 = age2;
    }

    public String getAge3() {
        return age3;
    }

    public void setAge3(String age3) {
        this.age3 = age3;
    }

    public String getAge4() {
        return age4;
    }

    public void setAge4(String age4) {
        this.age4 = age4;
    }

    public String getAge5() {
        return age5;
    }

    public void setAge5(String age5) {
        this.age5 = age5;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public int compareTo(StepTwoMapBean o) {
        if(o.getMdn().compareTo(this.mdn)==0){
            if(o.getStatus().compareTo(this.status)==0){
                //参与比较的两条数据都是从output1中读的
                //按appid排序即可
                return o.getAppId().compareTo(this.appId);
            }else{
                //参与比较的两条数据一个是从output中读的，一个是从hbase中读的
                //让hbase中读的信息排前边
                if("fromhbase".equals(o.getStatus())){
                    return 1;
                }else{
                    return -1;
                }
            }
        }else{
            return o.getMdn().compareTo(this.mdn);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.mdn);
        out.writeUTF(this.appId);
        out.writeUTF(this.times);
        out.writeUTF(this.male);
        out.writeUTF(this.female);
        out.writeUTF(this.age1);
        out.writeUTF(this.age2);
        out.writeUTF(this.age3);
        out.writeUTF(this.age4);
        out.writeUTF(this.age5);
        out.writeUTF(this.status);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.mdn = in.readUTF();
        this.appId = in.readUTF();
        this.times = in.readUTF();
        this.male = in.readUTF();
        this.female = in.readUTF();
        this.age1 = in.readUTF();
        this.age2 = in.readUTF();
        this.age3 = in.readUTF();
        this.age4 = in.readUTF();
        this.age5 = in.readUTF();
        this.status = in.readUTF();
    }

    public void set(String mdn, String appId, String times, String male, String female, String age1, String age2, String age3, String age4, String age5) {
        this.mdn = mdn;
        this.appId = appId;
        this.times = times;
        this.male = male;
        this.female = female;
        this.age1 = age1;
        this.age2 = age2;
        this.age3 = age3;
        this.age4 = age4;
        this.age5 = age5;
    }

    public StepTwoMapBean(String status) {
        this.status = status;
    }


    public StepTwoMapBean() {
    }
}
