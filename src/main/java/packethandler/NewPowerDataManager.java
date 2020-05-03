package packethandler;

import HelperThreads.*;
import Helperclasses.CrcGenerator;
import Helperclasses.Maps;
import Helperclasses.RedisPojo;
import Helperclasses.Utility;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.*;


public class NewPowerDataManager implements Runnable {
    static Logger log = Logger.getLogger("powerLogger");
    static Logger log1 = Logger.getLogger("errorLogger");
    static Logger log2 = Logger.getLogger("exceptionLogger");
    String logdata = "";

    static HashMap<String,String> bridgeMap;
    static HashMap<String,String> deviceMap;
    static HashMap<String,String> alertMap;

    BlockingQueue<RedisPojo> queue;
    BlockingQueue<String> pgsqlqueue;

    byte[] packet=null;
    long starttime;



    public  NewPowerDataManager(byte[] packet,long starttime){
        this.packet=packet;
        this.starttime=starttime;
        queue= RedisThreads.getinstance().RedisdataQueue;
        pgsqlqueue= PgsqlThreads.getinstance().PgsqldataQueue;


    }

    public void run() {

        try {


            StringBuffer result4 = new StringBuffer();
            for (byte b : packet) {
                result4.append(String.format("%02X ", b));
                result4.append(" "); // delimiter
            }

            long Pkttimestamp=System.currentTimeMillis() / 1000;

            if(packet.length==32){
                Pkttimestamp=((packet[28] & 0xff) << 24 | (packet[29] & 0xff) << 16| (packet[30] & 0xff) << 8 | (packet[31] & 0xff));
                packet= Arrays.copyOfRange(packet,0,28);
            }

            bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
            deviceMap= Deviceverifier.getinstance().FullDeviceMap;
            alertMap= AlertSettingThread.getinstance().FullAlertMap;



            String logstring = "";

            logdata = logdata + "{";
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
                packetData = Arrays.copyOfRange(packet, START_OPERATION_INDEX, packet.length);
                byte[] decryptedPacket = null;
                byte[] reversedPacket = null;
                try {
                    reversedPacket = Utility.getInstance().reverseMask(packetData);
                    decryptedPacket = Utility.getInstance().decrypt(reversedPacket, networkKey);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    log2.fatal(e);
                    log2.fatal("decrypt" + e.getMessage());

                }

                byte[] crc = CrcGenerator.calculateCrc(Arrays.copyOfRange(decryptedPacket, 2, decryptedPacket.length));


                StringBuffer result1 = new StringBuffer();
                for (byte b : packetData) {
                    result1.append(String.format("%02X ", b));
                    result1.append(" "); // delimiter
                }

                StringBuffer result2 = new StringBuffer();
                for (byte b : decryptedPacket) {
                    result2.append(String.format("%02X ", b));
                    result2.append(" "); // delimiter
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



                        if (Maps.getInstance().sequencenumber((long) deviceLongId,sequenceNum)) {
                            if (commandByte == STATUS_DOCKPMD_COMMAND) {




                                Long time = System.currentTimeMillis() / 1000;

                                time = time * 1000000 + (LocalDateTime.now().getLong(ChronoField.MICRO_OF_SECOND));

                                String portId = "";


                                switch (statusId) {
                                    case NEWPOWER_DATA:
                                        double power = ((decryptedPacket[11] & 0xff) << 32 | (decryptedPacket[12] & 0xff) << 24 | (decryptedPacket[13] & 0xff) << 16 | (decryptedPacket[14] & 0xff) << 8 | (decryptedPacket[15] & 0xff));

                                        int alertType = 5;
                                        String jedisKey = "{ASC}AT" + alertType + "RO" + rootOrgId;

                                        if (decryptedPacket[10] == 0) {

                                            power = power / (1000 * 1000);

                                            if (alertMap.containsKey(jedisKey)) {
                                                String[] vals = alertMap.get(jedisKey).split(":");
                                                double alertLevel = Double.parseDouble(vals[2]);
                                                if (power > alertLevel) {

                                                    int category = 2;
                                                    int customerId = 1;

                                                    double range = power;


                                                    String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;

//
                                                    String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
                                                            regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + deviceLongId + ":" + deviceType + ":" + range + ":" + time;
                                                    queue.put(new RedisPojo(alertKey,alertVal));
                                                }
                                            }
                                            portId = dvc[12];

                                            queue.put(new RedisPojo("{PMDPSTAT}RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId + "PI" + portId, portId + ":" + timeStamp));

                                            String sql = "INSERT INTO wiseconnect.tbl_power_consumption "
                                                    + "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,port_id,timestamp,power_consumed,seq) VALUES ("
                                                    + rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId + "," + portId + "," + Pkttimestamp + "," + power +","+sequenceNum+ ")";

                                            pgsqlqueue.put(sql);
                                            if(POWER_LOG==1) {
                                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"power\":" + power + "}";
                                                log.info(logstring);
                                            }
                                        } else {

                                            //power = power / 1000;

                                            power = power / (1000 * 1000);

                                            if (alertMap.containsKey(jedisKey)) {
                                                String[] vals = alertMap.get(jedisKey).split(":");
                                                double alertLevel = Double.parseDouble(vals[2]);
                                                if (power > alertLevel) {


                                                    int category = 2;
                                                    int customerId = 1;

                                                    double range = power;


                                                    String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;


                                                    String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
                                                            regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + deviceLongId + ":" + deviceType + ":" + range +  ":" + time;
                                                    queue.put(new RedisPojo(alertKey,alertVal));
                                                }
                                            }

                                            int portIdDevice = decryptedPacket[10];
                                            if (portIdDevice == 1) {
                                                //portId = dvc[5];
                                                portId = dvc[12];
                                            } else if (portIdDevice == 2) {
                                                //portId = dvc[6];
                                                portId = dvc[13];
                                            } else if (portIdDevice == 3) {
                                                //portId = dvc[7];
                                                portId = dvc[14];
                                            } else if (portIdDevice == 4) {
                                                //portId = dvc[8];
                                                portId = dvc[15];
                                            }

                                            queue.put(new RedisPojo("{PMDPSTAT}RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId + "PI" + portId, portId + ":" + timeStamp));

                                            String sql = "INSERT INTO wiseconnect.tbl_power_consumption "
                                                    + "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,port_id,timestamp,power_consumed,seq) VALUES ("
                                                    + rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId + "," + portId + "," + Pkttimestamp + "," + power +","+sequenceNum+ ")";

                                            pgsqlqueue.put(sql);
                                            if(POWER_LOG==1) {
                                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"power\":" + power + "}";
                                                log.info(logstring);
                                            }
                                        }

                                        queue.put(new RedisPojo("{POWER}PI" + portId, power + ""));
                                        break;

                                }



                            } else {
                                if(POWER_LOG==1) {
                                    logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "another command" + "}";
                                    log.info(logstring);
                                }


                            }

                        } else {
                            if(POWER_LOG==1) {
                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis()  +  ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+",\"first\":"+(starttime)+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "sequence number mismatch packet" + sequenceNum +  "}";
                                log.info(logstring);
                            }
                        }
                    } else {
                        if(POWER_LOG==1) {
                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis() + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "dzkey mismatch" + dzKey + "}";
                            log.info(logstring);
                        }





                    }

                } else {
                    if(POWER_LOG==1) {
                        logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+ ",\"first\":"+(starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"status\":" + "crc mismatch}";
                        log.info(logstring);
                    }
                }

            } else {
                if(POWER_LOG==1) {
                    logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"power\",\"epoch\":" + System.currentTimeMillis() + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+ ",\"first\":"+(starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\"" + ",\"status\":" + "client key missing" + clientKey + "}";
                    log.info(logstring);
                }




            }
        }catch (Exception e){
            e.printStackTrace();
            log2.fatal("exception in tspd\n"+e);
        }
    }
}

