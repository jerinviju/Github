package HelperThreads;


import Connectionclasses.RedisDbFactoryClient;
import Helperclasses.RedisPojo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisThreads implements Runnable {


    public BlockingQueue<RedisPojo> RedisdataQueue;
    ExecutorService executor =null;
    static RedisThreads redisThreads=null;
    private static final Object mutex = new Object();

    private RedisThreads(){
        try {
            executor= Executors.newFixedThreadPool(5);
            RedisdataQueue = new ArrayBlockingQueue<>(500000);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static RedisThreads getinstance(){
        if(redisThreads==null){
            synchronized (mutex) {
                redisThreads = new RedisThreads();
            }
        }
        return redisThreads;
    }

    @Override
    public void run() {
        while(true){
            try{
                List<RedisPojo> sqlStrings = new ArrayList<>();
                RedisdataQueue.drainTo(sqlStrings);
                if(sqlStrings.size()>0) {
                    Task task = new Task(RedisDbFactoryClient.getInstance().getConnection(), sqlStrings);
                    executor.execute(task);
                }
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    class Task implements  Runnable{

        Jedis jedis;
        Statement stmt;
        List<RedisPojo> redisdata;
        Pipeline pipeline = null;

        private Task(Jedis jedis,List<RedisPojo> redisdata){
            this.jedis=jedis;

            this.redisdata=redisdata;
        }

        @Override
        public void run() {
            try {

                if(jedis!=null) {
                    pipeline = jedis.pipelined();
                    for (RedisPojo pojo : redisdata
                    ) {
                        if(pojo.getExpiry()==-1) {
                            pipeline.set(pojo.getKey(), pojo.getValue());
                        }else{
                            pipeline.setex(pojo.getKey(),pojo.getExpiry(),pojo.getValue());
                        }
                    }
                    pipeline.sync();
                }else{
                    for (RedisPojo pojo : redisdata
                    ) {
                        RedisdataQueue.put(pojo);
                    }
                }
            }catch (Exception e){

            }

        }

    }

}
