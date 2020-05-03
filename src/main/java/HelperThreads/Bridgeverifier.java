package HelperThreads;

import Connectionclasses.RedisDbFactoryClient;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Bridgeverifier implements Runnable {

    public static Bridgeverifier bridgeverifier;
    Jedis jedis=null;
    volatile public static HashMap<String,String> FullBridgeMap=new HashMap<>();
    private static HashMap<String,String> BridgeMap=new HashMap<>();
    private static final Object mutex = new Object();


    private Bridgeverifier(){
        jedis= RedisDbFactoryClient.getInstance().getConnection();
        Set<String> keys=jedis.keys("{CLIENT_CACHE}CI*");
        String[] bridgekeysArr = keys.stream().toArray(String[]::new);
        if(bridgekeysArr.length>0){
            List<String> bridgeData = jedis.mget(bridgekeysArr);
            for(int i=0;i<bridgekeysArr.length;i++){
                FullBridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
                BridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
            }

        }
    }

    public static Bridgeverifier getinstance(){
        if(bridgeverifier==null){
            synchronized (mutex) {
                bridgeverifier = new Bridgeverifier();
            }
        }
        return bridgeverifier;
    }

    @Override
    public void run() {

        try{
            while(true) {
                Set<String> keys=jedis.keys("{CLIENT_CACHE}CI*");
                String[] bridgekeysArr = keys.stream().toArray(String[]::new);
                if(bridgekeysArr.length>0){
                    List<String> bridgeData = jedis.mget(bridgekeysArr);
                    for(int i=0;i<bridgekeysArr.length;i++){

                        if(BridgeMap.containsKey(bridgekeysArr[i])){
                            if(BridgeMap.get(bridgekeysArr[i]).equals(bridgeData.get(i))){
                                continue;
                            }else{
                                BridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
                                FullBridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
                            }
                        }else{
                            BridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
                            FullBridgeMap.put(bridgekeysArr[i],bridgeData.get(i));
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
