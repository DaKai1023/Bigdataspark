package com.wanglikai.redisutil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

public class JedisConnectionPool {

    private static JedisPoolConfig config = null;
    private  static JedisPool jedisPool = null;

    static {
        config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(10);
        //设置调用borrow Object方法，是否进行有效性检查
        config.setTestOnBorrow(true);
        //设置最大空闲连接数
        config.setMaxIdle(5);
    }

    public static Jedis getConnection(){

        if(jedisPool ==null){
            jedisPool = new JedisPool(config,"192.168.56.104",6379,10000,"123456");
        }
            return jedisPool.getResource();
    }

    //测试
    public static void main(String[] args) {
        Jedis conn = JedisConnectionPool.getConnection();

        Set<String> keys = conn.keys("*");
        for(String key : keys){
            System.out.println(key);
        }
        conn.close();
    }
}
