package Connectionclasses;

import Helperclasses.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisDbFactoryClient {
    private static RedisDbFactoryClient redisDbFactoryClient;
    private  JedisPool jedisPool;
    private static final Object mutex = new Object();

    private RedisDbFactoryClient(){
        JedisPoolConfig poolConfig= buildPoolConfig();
       jedisPool= new JedisPool(poolConfig, Constants.HOST);
    }
     public static RedisDbFactoryClient getInstance(){
        if(redisDbFactoryClient== null){
            synchronized (mutex) {
                redisDbFactoryClient = new RedisDbFactoryClient();
            }
        }
        return  redisDbFactoryClient;
     }

    private JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(20);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    public Jedis getConnection(){

            Jedis jedis=jedisPool.getResource();
            if(jedis.isConnected()){
                return jedis;
            }
            return null;
    }
}
