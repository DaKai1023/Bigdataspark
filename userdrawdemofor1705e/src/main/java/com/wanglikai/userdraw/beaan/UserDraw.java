package com.wanglikai.userdraw.beaan;

import java.math.BigDecimal;

//用户画像类
public class UserDraw {

    //用户手机号（用户唯一标识）
    private String MDN;
    //男性权重
    private Double male;
    //女性权重
    private Double female;
    //年龄段1 24岁以下
    private Double age1;
    //年龄段2 25-30岁
    private Double age2;
    //年龄段3 31-35岁
    private Double age3;
    //年龄段4 36-40岁
    private Double age4;
    //年龄段5 40岁以上
    private Double age5;

    //初始化性别概率
    public void initSex(Double male, Double female){
        Double sum = male + female;
        if(sum != 0) {
            this.male = male / sum;
            this.female = female /sum;
        }
    }

    //对男女性别进行计算
    public void protraitSex(Double male, Double female, Long times) {
        Double sum = (this.male + this.female +(male + female) * times);
        if(sum != 0) {
            this.male = (this.male + male * times) / sum;
            this.female = (this.female + female * times) / sum;
        }
    }

    //初始化年龄段概率
    public void initAge(Double age1, Double age2,Double age3,Double age4,Double age5) {
        Double sum = age1 + age2 + age3 + age4 + age5;
        if(sum != 0) {
            this.age1 = age1 / sum;
            this.age2 = age2 / sum;
            this.age3 = age3 / sum;
            this.age4 = age4 / sum;
            this.age5 = age5 / sum;
        }
    }

    //对年龄段概率进行计算
    public void protraitAge(Double age1,Double age2,Double age3,Double age4,Double age5, Long times) {
        Double sum = (this.age1 + this.age2 + this.age3 + this.age4 + this.age5 + (age1 + age2 + age3 + age4 + age5) * times);

        this.age1 = (this.age1 + age1 * times) / sum;
        this.age2 = (this.age2 + age2 * times) / sum;
        this.age3 = (this.age3 + age3 * times) / sum;
        this.age4 = (this.age4 + age4 * times) / sum;
        this.age5 = (this.age5 + age5 * times) / sum;
    }

    public String getMDN() {
        return MDN;
    }

    public void setMDN(String MDN) {
        this.MDN = MDN;
    }

    public Double getMale() {
        return male;
    }

    public void setMale(Double male) {
        this.male = male;
    }

    public Double getFemale() {
        return female;
    }

    public void setFemale(Double female) {
        this.female = female;
    }

    public Double getAge1() {
        return age1;
    }

    public void setAge1(Double age1) {
        this.age1 = age1;
    }

    public Double getAge2() {
        return age2;
    }

    public void setAge2(Double age2) {
        this.age2 = age2;
    }

    public Double getAge3() {
        return age3;
    }

    public void setAge3(Double age3) {
        this.age3 = age3;
    }

    public Double getAge4() {
        return age4;
    }

    public void setAge4(Double age4) {
        this.age4 = age4;
    }

    public Double getAge5() {
        return age5;
    }

    public void setAge5(Double age5) {
        this.age5 = age5;
    }

    public UserDraw(String MDN, Double male, Double female, Double age1, Double age2, Double age3, Double age4, Double age5) {
        this.MDN = MDN;
        this.male = male;
        this.female = female;
        this.age1 = age1;
        this.age2 = age2;
        this.age3 = age3;
        this.age4 = age4;
        this.age5 = age5;
    }

    @Override
    public String toString() {

        StringBuffer sb = new StringBuffer();
        sb.append(this.MDN + "|");
        sb.append(new BigDecimal(this.male).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.female).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.age1).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.age2).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.age3).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.age4).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");
        sb.append(new BigDecimal(this.age5).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue() + "|");

        return sb.toString();
    }
}
