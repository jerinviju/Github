package HelperThreads;

import Connectionclasses.RedisDbFactoryClient;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Set;


public class AlertSettingThread implements Runnable {

    public volatile static AlertSettingThread alertSettingThread;
    Jedis jedis=null;
    volatile public static HashMap<String,String> FullAlertMap=new HashMap<>();
    private static HashMap<String,String> AlertMap=new HashMap<>();
    private static final Object mutex = new Object();


    private AlertSettingThread(){
        jedis= RedisDbFactoryClient.getInstance().getConnection();
        Set<String> keys=jedis.keys("{ASC}AT*");
        String[] alertkeysArr = keys.stream().toArray(String[]::new);
        if(alertkeysArr.length>0){
            List<String> alertData = jedis.mget(alertkeysArr);
            for(int i=0;i<alertkeysArr.length;i++){
                FullAlertMap.put(alertkeysArr[i],alertData.get(i));
                AlertMap.put(alertkeysArr[i],alertData.get(i));
            }

        }
    }

    public static AlertSettingThread getinstance(){
        if(alertSettingThread==null){
            synchronized (mutex) {
                alertSettingThread = new AlertSettingThread();
            }
        }
        return alertSettingThread;
    }

    @Override
    public void run() {

        try{
            while(true) {
                Set<String> keys=jedis.keys("{ASC}AT**");
                String[] alertkeysArr = keys.stream().toArray(String[]::new);
                if(alertkeysArr.length>0){
                    List<String> alertData = jedis.mget(alertkeysArr);
                    for(int i=0;i<alertkeysArr.length;i++){

                        if(AlertMap.containsKey(alertkeysArr[i])){
                            if(AlertMap.get(alertkeysArr[i]).equals(alertData.get(i))){
                                continue;
                            }else{
                                AlertMap.put(alertkeysArr[i],alertData.get(i));
                                FullAlertMap.put(alertkeysArr[i],alertData.get(i));
                            }
                        }else{
                            AlertMap.put(alertkeysArr[i],alertData.get(i));
                            FullAlertMap.put(alertkeysArr[i],alertData.get(i));
                        }
                    }

                }
                Thread.sleep(5000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
