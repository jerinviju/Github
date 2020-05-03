package packethandler;

import HelperThreads.Bridgeverifier;
import HelperThreads.Deviceverifier;
import HelperThreads.PgsqlThreads;
import HelperThreads.RedisThreads;
import Helperclasses.*;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.USB_STATUS_LOG;


public class UsbStatusdataManager implements Runnable{

    static Logger log = Logger.getLogger("ustatusLogger");
    static Logger log2 = Logger.getLogger("exceptionLogger");

    static HashMap<String,String> bridgeMap;
    static HashMap<String,String> deviceMap;

    BlockingQueue<RedisPojo> queue;
    BlockingQueue<String> pgsqlqueue;

    byte[] packet=null;
    long starttime;


    public  UsbStatusdataManager(byte[] packet,long starttime){
        this.packet=packet;
        this.starttime=starttime;
        queue= RedisThreads.getinstance().RedisdataQueue;
        pgsqlqueue= PgsqlThreads.getinstance().PgsqldataQueue;

    }


    private String[] queryBuilder(String sql, int id, int status, String [] det, long currtime){

        String query = "";

        int flag = 0;

        int prevstatus = Integer.parseInt(det[1]);
        long prevtime = Integer.parseInt(det[2]);
        long diff = currtime - prevtime;

        if(prevstatus == 0 && status == 0) {
            query = sql + id + ","+ currtime + "," +0+")";
            diff = 0;
        }else if(prevstatus == 0 && status == 1) {
            query = sql + id + ","+ currtime + ","+0+")";
            diff = 0;
            flag = 1;
        }else if(prevstatus == 1 && status == 0) {
            query = sql + id + ","+ currtime  + "," +diff+")";
            flag = 1;
        }else if(prevstatus == 1 && status == 1) {
            query = sql + id + ","+ currtime + "," + diff+")";
        }

        return new String[] {String.valueOf(diff), query};

        //return  query;

    }



    public void run() {
        try {
            long Pkttimestamp=System.currentTimeMillis()/1000;

            StringBuffer result4 = new StringBuffer();
            for (byte b : packet) {
                result4.append(String.format("%02X ", b));
                result4.append(" "); // delimiter
            }


            if(packet.length==32){
                Pkttimestamp=((packet[28] & 0xff) << 24 | (packet[29] & 0xff) << 16| (packet[30] & 0xff) << 8 | (packet[31] & 0xff));
                packet= Arrays.copyOfRange(packet,0,28);
            }

            bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
            deviceMap= Deviceverifier.getinstance().FullDeviceMap;


            String logstring = "";
            int rootOrgId = ((packet[3] & 0xff) << 8 | (packet[4] & 0xff));
            int zoneId = ((packet[5] & 0xff) << 8 | (packet[6] & 0xff));
            int bridgeId = ((packet[7] & 0xff) << 8 | (packet[8] & 0xff));


            int bridgeLongId = 0;

            int deviceLongId = 0;

            String networkKeyStr = "";
            byte[] packetData = {};
            String redisKey = "";
            String redisData = "";

            long timeStamp = System.currentTimeMillis() / 1000;

            String clientKey = "{CLIENT_CACHE}CI" + bridgeId;
            if (bridgeMap.containsKey(clientKey)) {
                String[] userd = bridgeMap.get(clientKey).split(":");
                bridgeLongId = Integer.parseInt(userd[0]);

                networkKeyStr = userd[2];

            }


            if (bridgeLongId != 0) {

                byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);

                packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX, packet.length);
                byte[] decryptedPacket = null;
                byte[] reversedPacket = null;
                try {
                    reversedPacket = Utility.getInstance().reverseMask(packetData);
                    decryptedPacket = Utility.getInstance().decrypt(reversedPacket, networkKey);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    log2.fatal(e);
                    log2.fatal("packet decrypt exception in tsts");

                }

                byte[] crc = CrcGenerator.calculateCrc(Arrays.copyOfRange(decryptedPacket, 2, decryptedPacket.length));
                StringBuffer result1 = new StringBuffer();
                for (byte b : packetData) {
                    result1.append(String.format("%02X ", b));
                    result1.append(" "); // delimiter
                }

                StringBuffer result2 = new StringBuffer();
                for (byte b : reversedPacket) {
                    result2.append(String.format("%02X ", b));
                    result2.append(" "); // delimiter
                }

                StringBuffer result3 = new StringBuffer();
                for (byte b : decryptedPacket) {
                    result3.append(String.format("%02X ", b));
                    result3.append(" "); // delimiter
                }


                if (((crc[0] & 0xff) == (decryptedPacket[0] & 0xff)) && ((crc[1] & 0xff) == (decryptedPacket[1] & 0xff))) {
                    int deviceShortId = ((decryptedPacket[4] & 0xff) << 4) | (decryptedPacket[3] & 0xff);
                    int sequenceNum = ((decryptedPacket[6] & 0xff) << 8) + (decryptedPacket[7] & 0xff);


                    String dzKey = "{DZC}ZI" + zoneId + "D" + deviceShortId;



                    if (deviceMap.containsKey(dzKey)) {
                        String[] dvc = deviceMap.get(dzKey).split(":");
                        deviceLongId = Integer.parseInt(dvc[0]);
                        String regionId = dvc[8];
                        String officeId = dvc[9];
                        String floorId = dvc[10];
                        String wsId = dvc[11];
                        String deviceType = dvc[4];


                        redisKey = "{LRC}" + "DI" + deviceLongId;
                        redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + timeStamp + ":1";

                        queue.put(new RedisPojo(redisKey,redisData));

                        int commandByte = decryptedPacket[8] & 0xff;
                        int statusId = decryptedPacket[9] & 127;


                        if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {
                            String pid = "";
                            String vid = "";
                            int usb_connected = 0;
                            int portid = 0;

                            switch (statusId) {
                                case Constants.USB_STATUS:
                                    StringBuffer pidbuffer = new StringBuffer();
                                    pidbuffer.append(String.format("%02X", decryptedPacket[14]));
                                    pidbuffer.append(String.format("%02X", decryptedPacket[15]));

                                    StringBuffer vidbuffer = new StringBuffer();
                                    vidbuffer.append(String.format("%02X", decryptedPacket[12]));
                                    vidbuffer.append(String.format("%02X", decryptedPacket[13]));
                                    pid = pidbuffer.toString().toLowerCase();
                                    vid = vidbuffer.toString().toLowerCase();
                                    usb_connected = (decryptedPacket[11] & 0xff);
                                    portid = (decryptedPacket[10] & 0xff);
                                    if(USB_STATUS_LOG==1) {
                                        if (portid == 0) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"0000,0000\",\"usb1\":\"0000,0000\",\"usb2\":\"0000,0000\",\"usb3\":\"0000,0000\",\"usb4\":\"0000,0000\"" + ",\"status\":" + "sucess" + "}";
                                        } else if (portid == 1) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"" + vid + "," + pid + "\",\"usb1\":\"0000,0000\",\"usb2\":\"0000,0000\",\"usb3\":\"0000,0000\",\"usb4\":\"0000,0000\"" + ",\"status\":" + "sucess" + "}";
                                        } else if (portid == 2) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"0000,0000\",\"usb1\":\"" + vid + "," + pid + "\",\"usb2\":\"0000,0000\",\"usb3\":\"0000,0000\",\"usb4\":\"0000,0000\"" + ",\"status\":" + "sucess" + "}";
                                        } else if (portid == 3) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"0000,0000\",\"usb1\":\"0000,0000\",\"usb2\":\"" + vid + "," + pid + "\",\"usb3\":\"0000,0000\",\"usb4\":\"0000,0000\"" + ",\"status\":" + "sucess" + "}";
                                        } else if (portid == 4) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"0000,0000\",\"usb1\":\"0000,0000\",\"usb2\":\"0000,0000\",\"usb3\":\"" + vid + "," + pid + "\",\"usb4\":\"0000,0000\"" + ",\"status\":" + "sucess" + "}";
                                        } else if (portid == 5) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"usb0\":\"0000,0000\",\"usb1\":\"0000,0000\",\"usb2\":\"0000,0000\",\"usb3\":\"0000,0000\",\"usb4\":\"" + vid + "," + pid + "\"" + ",\"status\":" + "sucess" + "}";
                                        }
                                        log.info(logstring);
                                    }


                                    String usbCacheKey = "{USBSTAT}RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "PI";
                                    long diff = 0;

                                    if (pid.equals("0000") && vid.equals("0000") && usb_connected == 0 && portid == 0) {
                                        queue.put(new RedisPojo(usbCacheKey + "1", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));
                                        queue.put(new RedisPojo(usbCacheKey + "2", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));
                                        queue.put(new RedisPojo(usbCacheKey + "3", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));
                                        queue.put(new RedisPojo(usbCacheKey + "4", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));
                                        queue.put(new RedisPojo(usbCacheKey + "5", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));

                                        Maps.getInstance().setUsb(usbCacheKey + "1", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp);
                                        Maps.getInstance().setUsb(usbCacheKey + "2", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp);
                                        Maps.getInstance().setUsb(usbCacheKey + "3", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp);
                                        Maps.getInstance().setUsb(usbCacheKey + "4", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp);
                                        Maps.getInstance().setUsb(usbCacheKey + "5", portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp);

                                    } else {
                                        if (Maps.getInstance().containsUsbkey(usbCacheKey + portid)) {
                                            String usbCacheValue =Maps.getInstance().getUsb(usbCacheKey + portid);
                                            long prevtime = Long.parseLong(usbCacheValue.split(":")[4]);
                                            queue.put(new RedisPojo(usbCacheKey + portid, portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));

                                            if (pid.equals("0000") && vid.equals("0000")) {
                                            } else {

                                                try {

                                                    diff = timeStamp - prevtime;

                                                    String sql = "INSERT INTO wiseconnect.tbl_port_usage "
                                                            + "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,seq,port_id,pid,vid,timestamp,duration) VALUES ("
                                                            + rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId+ "," +sequenceNum + "," + portid + ",'" + pid + "','" + vid + "'," + Pkttimestamp + "," + diff + ")";

                                                    pgsqlqueue.put(sql);
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                    log2.fatal(e);
                                                    log2.fatal("cmysql exception");
                                                }
                                            }


                                        } else {

                                            queue.put(new RedisPojo(usbCacheKey + portid, portid + ":" + usb_connected + ":" + pid + ":" + vid + ":" + timeStamp));
                                            if (pid.equals("0000") && vid.equals("0000")) {
                                            } else {
                                                try {

                                                    String sql = "INSERT INTO wiseconnect.tbl_port_usage "
                                                            + "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,seq,port_id,pid,vid,timestamp,duration) VALUES ("
                                                            + rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId + ","+sequenceNum+"," + portid + ",'" + pid + "','" + vid + "'," + timeStamp + ",0)";

                                                    pgsqlqueue.put(sql);
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                    log2.fatal(e);
                                                    log2.fatal("cmysql exception");

                                                }
                                            }
                                        }
                                    }


                                    break;

                            }
                        } else {
                            if(USB_STATUS_LOG==1) {
                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "command is wrong need 128 got" + commandByte + "}";
                                log.info(logstring);
                            }

                        }

                    } else {
                        if(USB_STATUS_LOG==1) {
                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "dzc wrong got" + dzKey + "}";
                            log.info(logstring);
                        }
                    }
                } else {
                    if(USB_STATUS_LOG==1) {
                        logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result3.toString() + ",\"status\":" + "crc error }";
                        log.info(logstring);
                    }

                }
            } else {
                if(USB_STATUS_LOG==1) {
                    logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"usb\",\"epoch\":" + System.currentTimeMillis()+",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + ",\"status\":" + "clientkey  wrong got" + clientKey + "}";
                    log.info(logstring);
                }
            }

        }catch (Exception e){
            e.printStackTrace();
            log2.fatal("exception in tsst parsing\n"+e);


        }
    }
}





