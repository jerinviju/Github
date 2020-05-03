package HelperThreads;

import Connectionclasses.RedisDbFactoryClient;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Deviceverifier implements Runnable {

    Jedis jedis=null;
    public  static Deviceverifier deviceverifier;
    volatile public static HashMap<String,String> FullDeviceMap=new HashMap<>();
    private static HashMap<String,String> DeviceMap=new HashMap<>();
    private static final Object mutex = new Object();

    private Deviceverifier(){
        jedis= RedisDbFactoryClient.getInstance().getConnection();
        Set<String> keys=jedis.keys("{DZC}ZI*");
        String[] bridgekeysArr = keys.stream().toArray(String[]::new);
        if(bridgekeysArr.length>0){
            List<String> bridgeData = jedis.mget(bridgekeysArr);
            for(int i=0;i<bridgekeysArr.length;i++){
                FullDeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
                DeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
            }

        }
    }

    public static Deviceverifier getinstance(){
        if(deviceverifier==null){
            synchronized (mutex) {
                deviceverifier = new Deviceverifier();
            }
        }
        return deviceverifier;
    }

    @Override
    public void run() {

        try{
            while(true) {
                Set<String> keys=jedis.keys("{DZC}ZI*");
                String[] bridgekeysArr = keys.stream().toArray(String[]::new);
                if(bridgekeysArr.length>0){
                    List<String> bridgeData = jedis.mget(bridgekeysArr);
                    for(int i=0;i<bridgekeysArr.length;i++){

                        if(DeviceMap.containsKey(bridgekeysArr[i])){
                            if(DeviceMap.get(bridgekeysArr[i]).equals(bridgeData.get(i))){
                                continue;
                            }else{
                                DeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
                                FullDeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
                            }
                        }else{
                            DeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
                            FullDeviceMap.put(bridgekeysArr[i],bridgeData.get(i));
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
