package Helperclasses;

import java.util.HashMap;

public class Maps {

    private volatile static Maps maps;
    private static final Object mutex = new Object();

    public  volatile static HashMap<Long,Integer> Device_sequnce_map;
    public  volatile static HashMap<String,String> Device_status_map;
    public  volatile static HashMap<String,String> RFID_map;
    public  volatile static HashMap<String,String> USB_map;

    private Maps(){
        Device_sequnce_map=new HashMap<>();
        Device_status_map=new HashMap<>();
        RFID_map=new HashMap<>();
        USB_map=new HashMap<>();
    }

    public  static Maps getInstance(){
        if(maps == null){
            synchronized (mutex){
                maps = new Maps();
            }
        }
        return maps;
    }

    public synchronized  boolean sequencenumber(long deviceid,int sequencenumber){
        int seq=-1;
        if (Device_sequnce_map.containsKey( deviceid)) {
            seq = Device_sequnce_map.get(deviceid);
        } else {
            Device_sequnce_map.put(deviceid, sequencenumber );
            return true;
        }
        if (seq == 65535 || (seq - sequencenumber) > 1000) {
            seq = 1;
        }
        if(seq<sequencenumber){
            Device_sequnce_map.put(deviceid, sequencenumber );
            return true;
        }

        return false;
    }

    public synchronized String getStatus(String Key){
        if(Device_status_map.containsKey(Key)){
            return Device_status_map.get(Key);
        }
        return "";
    }

    public synchronized String getRfid(String Key){
        if(RFID_map.containsKey(Key)){
            String[] data=RFID_map.get(Key).split("TIME");
            long expirytime=Long.parseLong(data[1]);
            if((System.currentTimeMillis()/1000-expirytime)>30){
                return "";
            }else{
                return data[0];
            }
        }
        return "";
    }

    public synchronized void setRfid(String Key,String Val){
        RFID_map.put(Key,Val+"TIME"+System.currentTimeMillis()/1000);
    }

    public synchronized void deleteRfid(String Key){
        RFID_map.remove(Key);
    }

    public synchronized void setStatus(String Key,String Val){
        Device_status_map.put(Key,Val);
    }

    public synchronized void setUsb(String Key,String Val){
        USB_map.put(Key,Val);
    }

    public synchronized String getUsb(String Key){
       if(USB_map.containsKey(Key)){
           return USB_map.get(Key);
       }
       return "";
    }

    public synchronized boolean containsUsbkey(String Key){
        if(USB_map.containsKey(Key)){
            return true;
        }
        return false;
    }





}
