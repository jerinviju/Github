package packethandler;

import HelperThreads.Bridgeverifier;
import HelperThreads.Deviceverifier;
import HelperThreads.RedisThreads;
import Helperclasses.Constants;
import Helperclasses.CrcGenerator;
import Helperclasses.RedisPojo;
import Helperclasses.Utility;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.VOLTAGE_LOG;


public class CurrentVoltageDataManager implements Runnable {

    static Logger log= Logger.getLogger("currentLogger");

    static Logger log2 = Logger.getLogger("exceptionLogger");

    BlockingQueue<RedisPojo> queue;
    static HashMap<String,String> bridgeMap;
    static HashMap<String,String> deviceMap;

    static HashMap<String,String> tempary_cur_vol;


    byte[] packet=null;
    long starttime;

    public CurrentVoltageDataManager(byte[] packet,long starttime) {
        this.packet=packet;
        this.starttime=starttime;
        tempary_cur_vol=new HashMap<>();
        queue= RedisThreads.getinstance().RedisdataQueue;
    }

    @Override
    public void run() {

        try {

            bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
            deviceMap= Deviceverifier.getinstance().FullDeviceMap;

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


                        if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {


                            switch (statusId) {
                                case Constants.CURRENT_DATA:

                                    int portid = (decryptedPacket[10] & 0xff);
                                    if(portid==1){
                                        Thread.sleep(10);
                                    }else if(portid==2){
                                        Thread.sleep(15);
                                    }else if(portid==3){
                                        Thread.sleep(5);
                                    }else if(portid==4){
                                        Thread.sleep(20);
                                    }
                                    float voltage = ((decryptedPacket[12] & 0xff) << 8 | (decryptedPacket[13] & 0xff));
                                    float current = ((decryptedPacket[14] & 0xff) << 8 | (decryptedPacket[15] & 0xff));
                                    if (decryptedPacket[10] == 0) {

                                        voltage = voltage / 1000;

                                        String key = "{CVC}WI" + wsId + "DI" + deviceLongId;
                                        String Value = wsId + ":" + deviceLongId + ":" + current + ":" + voltage + ":" + System.currentTimeMillis() / 1000;
                                        queue.put(new RedisPojo(key,Value));

                                        if(VOLTAGE_LOG==1) {
                                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis()  + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"voltage\":" + voltage + ",\"current\":" + current + ",\"status\":" + "success" + "}";
                                            log.info(logstring);
                                        }
                                    } else {

                                        voltage = (voltage * 10) / 1000;

                                        if (tempary_cur_vol.containsKey("{TMPVOL}DI" + deviceLongId) && tempary_cur_vol.containsKey("{TMPCUR}DI" + deviceLongId) ) {

                                            String[] tempVAl = tempary_cur_vol.get("{TMPVOL}DI" + deviceLongId).split(":");
                                            String[] tempVAl1 = tempary_cur_vol.get("{TMPCUR}DI" + deviceLongId).split(":");
                                            String val = "";
                                            String val1 = "";
                                            switch (portid) {
                                                case 1:
                                                    val = voltage + ":" + tempVAl[1] + ":" + tempVAl[2] + ":" + tempVAl[3];
                                                    val1 = current + ":" + tempVAl1[1] + ":" + tempVAl1[2] + ":" + tempVAl1[3];
                                                    break;
                                                case 2:
                                                    val = tempVAl[0] + ":" + voltage + ":" + tempVAl[2] + ":" + tempVAl[3];
                                                    val1 = tempVAl1[0] + ":" + current + ":" + tempVAl1[2] + ":" + tempVAl1[3];
                                                    break;
                                                case 3:
                                                    val = tempVAl[0] + ":" + tempVAl[1] + ":" + voltage + ":" + tempVAl[3];
                                                    val1 = tempVAl1[0] + ":" + tempVAl1[1] + ":" + current + ":" + tempVAl1[3];
                                                    break;
                                                case 4:
                                                    val = tempVAl[0] + ":" + tempVAl[1] + ":" + tempVAl[2] + ":" + voltage;
                                                    val1 = tempVAl1[0] + ":" + tempVAl1[1] + ":" + tempVAl1[2] + ":" + current;
                                                    break;
                                            }

                                            String[] valcheck = val.split(":");
                                            String[] valcheck1 = val1.split(":");
                                            int i;
                                            float totvoltage = 0;
                                            float totalcurrent = 0;


                                            for (i = 0; i < valcheck.length; i++) {
                                                if (Float.parseFloat(valcheck[i]) < 0 || Float.parseFloat(valcheck1[i]) < 0 ){

                                                    break;
                                                }
                                                totvoltage = totvoltage + Float.parseFloat(valcheck[i]);
                                                totalcurrent = totalcurrent + Float.parseFloat(valcheck1[i]);

                                            }
                                            if (i == valcheck.length) {

                                                totvoltage = totvoltage / 4;
                                                if(totalcurrent>=0 && totvoltage>=0) {

                                                    String key = "{CVC}WI" + wsId + "DI" + deviceLongId;
                                                    String Value = wsId + ":" + deviceLongId + ":" + totalcurrent + ":" + totvoltage + ":" + System.currentTimeMillis() / 1000;
                                                    queue.put(new RedisPojo(key,Value));
                                                }

                                                val = "-1:-1:-1:-1";
                                                val1 = "-1:-1:-1:-1";
                                                tempary_cur_vol.put("{TMPVOL}DI" + deviceLongId, val);
                                                tempary_cur_vol.put("{TMPCUR}DI" + deviceLongId, val1);



                                            } else {

                                                tempary_cur_vol.put("{TMPVOL}DI" + deviceLongId, val);
                                                tempary_cur_vol.put("{TMPCUR}DI" + deviceLongId, val1);
                                            }
                                        } else {

                                            String val = "";
                                            String val1 = "";
                                            switch (portid) {
                                                case 1:
                                                    val = voltage + ":-1:-1:-1";
                                                    val1 = current + ":-1:-1:-1";
                                                    break;
                                                case 2:
                                                    val = "-1:" + voltage + ":-1:-1";
                                                    val1 = "-1:" + current + ":-1:-1";
                                                    break;
                                                case 3:
                                                    val = "-1:-1:" + voltage + ":-1";
                                                    val1 = "-1:-1:" + current + ":-1";
                                                    break;
                                                case 4:
                                                    val = "-1:-1:-1:" + voltage;
                                                    val1 = "-1:-1:-1:" + current;
                                                    break;
                                            }

                                            tempary_cur_vol.put("{TMPVOL}DI" + deviceLongId, val);
                                            tempary_cur_vol.put("{TMPCUR}DI" + deviceLongId, val1);
                                        }
                                        if(VOLTAGE_LOG==1) {

                                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis() + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"voltage\":" + voltage + ",\"current\":" + current + ",\"status\":" + "succes "+"}";
                                                log.info(logstring);


                                        }
                                    }
                                    break;
                            }
                        } else {
                            if(VOLTAGE_LOG==1) {
                                logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis()  + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "command byte mismatch got " + commandByte + "}";
                                log.info(logstring);
                            }
                        }
                    } else {
                        if(VOLTAGE_LOG==1) {
                            logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis() + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "device cache miss need " + dzKey + "}";
                            log.info(logstring);
                        }


                    }
                } else {


                    if(VOLTAGE_LOG==1) {
                        logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"status\":" + "crc missmatch" + "}";
                        log.info(logstring);
                    }

                }

            } else {


                if(VOLTAGE_LOG==1) {
                    logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"voltcrnt\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"status\":" + "client cahce miss need " + clientKey + "}";
                    log.info(logstring);
                }


            }
        }catch (Exception e){
            e.printStackTrace();

            log2.fatal("exception in tsvc\n"+e);


        }


        }
    }

