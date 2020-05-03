package packethandler;


import Connectionclasses.MysqlDbFactoryClient;
import Connectionclasses.RedisDbFactoryClient;
import HelperThreads.Bridgeverifier;
import HelperThreads.Deviceverifier;
import Helperclasses.Constants;
import Helperclasses.CrcGenerator;
import Helperclasses.Utility;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;

import static Helperclasses.Constants.DEVICE_VERSION_LOG;


public class DeviceVersionManager implements Runnable{
    static Logger log= Logger.getLogger("bridgeLogger");

    static Logger log2 = Logger.getLogger("exceptionLogger");



    static HashMap<String,String> bridgeMap;
    static HashMap<String,String> deviceMap;

    byte[] packet=null;
    long starttime;


    public DeviceVersionManager(byte[] packet,long starttime){
        this.packet=packet;
        this.starttime=starttime;
    }


    @Override
    public void run() {

        try {

            bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
            deviceMap= Deviceverifier.getinstance().FullDeviceMap;

            Jedis jedis= RedisDbFactoryClient.getInstance().getConnection();
            Connection cmysql= MysqlDbFactoryClient.getInstance().getConnection();
            Statement stmt=null;

            long Pkttimestamp=0l;

            StringBuffer result4 = new StringBuffer();
            for (byte b : packet) {
                result4.append(String.format("%02X ", b));
                result4.append(" ");
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
            String networkKeyStr = "";
            byte[] packetData = {};
            int deviceLongId = 0;

            String clientKey = "{CLIENT_CACHE}CI" + bridgeId;
            if (bridgeMap.containsKey(clientKey)) {
                String[] userd = bridgeMap.get(clientKey).split(":");
                bridgeLongId = Integer.parseInt(userd[0]);
                networkKeyStr = userd[2];
            }

            if (bridgeLongId != 0) {

                byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);
                packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX + 1, packet.length - 2);

                StringBuffer result3 = new StringBuffer();
                for (byte b : packetData) {
                    result3.append(String.format("%02X ", b));
                    result3.append(" ");
                }

                byte[] decryptedPacket = null;
                try {
                    decryptedPacket = Utility.getInstance().decrypt(packetData, networkKey);
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

                        boolean flag = false;
                        String swversion1 = "";

                        int commandByte = decryptedPacket[8] & 0xff;
                        if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {



                            if ((decryptedPacket[9] & 0xff) == 14) {

                                String lsb = Integer.toHexString(decryptedPacket[11] & 0xff);
                                String mlsb = Integer.toHexString(decryptedPacket[12] & 0xff);
                                String msb = Integer.toHexString(decryptedPacket[13] & 0xff);
                                String swversion = msb + "." + mlsb + "." + lsb;

                                try {

                                    stmt = cmysql.createStatement();

                                    String sql = "UPDATE tbl_devices SET stm_sw_version=\"" + swversion + "\" ,timestamp = UNIX_TIMESTAMP(NOW(6)) WHERE  device_id=" + deviceLongId;

                                    stmt.addBatch(sql);
                                    stmt.executeBatch();
                                    stmt.close();

                                    stmt = cmysql.createStatement();
                                    String sql1 = "SELECT count(*) as count FROM tbl_devices WHERE stm_sw_version!= \"" + swversion + "\" AND zone_id= " + zoneId + " AND device_type= " + deviceType + ";";

                                    ResultSet rs = stmt.executeQuery(sql1);

                                    int count = 0;
                                    while (rs.next()) {
                                        count = rs.getInt("count");
                                    }

                                    rs.close();


                                    stmt.close();
                                    if (count == 0) {
                                        stmt = cmysql.createStatement();
                                        String sql12 = "INSERT INTO `tbl_ota_details_set_archive`(`ota_id`, `process_id`, `region_id`, `office_id`, `floor_id`, `zone_id`, `magic_number`,`device_type`, `status`, `reinitiate_count`, `device_count`, `parent_created_time`, `parent_updated_time`, `parent_timestamp`) "
                                                       + " SELECT tods.ota_id, tods.process_id, tods.region_id, tods.office_id, tods.floor_id, tods.zone_id, tods.magic_number, tods.device_type, tods.status, tods.reinitiate_count, (SELECT COUNT(*) FROM tbl_devices  AS td WHERE td.zone_id = "+zoneId+" AND td.device_type = "+deviceType+"), tods.created_time, tods.updated_time, tods.timestamp "
                                                       + " FROM tbl_ota_details_set AS tods"
                                                       + " WHERE tods.zone_id="+zoneId
                                                       + " AND tods.device_type="+deviceType+";";



                                        stmt.executeUpdate(sql12);
                                        String sql123 = "DELETE FROM tbl_ota_details_set "

                                                        + " WHERE zone_id="+zoneId
                                                        + " AND device_type="+deviceType+";";



                                        stmt.executeUpdate(sql123);
                                        stmt.close();
                                        int magicnum = Integer.parseInt(jedis.get("{OTA_MAGIC_NO}Z" + zoneId).split(":")[0]);
                                        String publishdata = stringbuilder(19, 2) + stringbuilder(5, 4) + stringbuilder(rootOrgId, 4) + stringbuilder(zoneId, 4) + stringbuilder(magicnum, 2);
                                        jedis.publish("complete", rootOrgId + ":" + zoneId + ":" + publishdata);

                                    }
                                    if(DEVICE_VERSION_LOG==1) {
                                        logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"stmversion\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"devicetype\":" + deviceType + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"value\":\"" + swversion + "\"" + ",\"status\":\"" + "sucess" + "\"}";
                                        log.info(logstring);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    log2.fatal(e);
                                    log2.fatal("sql exception in stm version update" + e.getMessage());

                                }





                            } else if ((decryptedPacket[10] & 0xff) == 15) {

                                String lsb = Integer.toHexString(decryptedPacket[11] & 0xff);
                                String mlsb = Integer.toHexString(decryptedPacket[12] & 0xff);
                                String msb = Integer.toHexString(decryptedPacket[13] & 0xff);
                                String swversion = msb + "." + mlsb + "." + lsb;

                                try {


                                    stmt = cmysql.createStatement();
                                    String sql = "UPDATE tbl_devices SET config_sw_version=\"" + swversion + "\" ,timestamp = UNIX_TIMESTAMP(NOW(6)) WHERE  device_id=" + deviceLongId;


                                    stmt.addBatch(sql);
                                    stmt.executeBatch();
                                    stmt.close();

                                    stmt = cmysql.createStatement();
                                    String sql1 = "SELECT count(*) as count FROM tbl_devices WHERE config_sw_version!=\"" + swversion + "\" AND zone_id= " + zoneId + " AND device_type= " + deviceType + ";";

                                    ResultSet rs = stmt.executeQuery(sql1);

                                    int count = 0;
                                    while (rs.next()) {
                                        count = rs.getInt("count");
                                    }

                                    rs.close();


                                    stmt.close();
                                    if (count == 0) {
                                        stmt = cmysql.createStatement();
                                        String sql12 = "INSERT INTO `tbl_ota_details_set_archive`(`ota_id`, `process_id`, `region_id`, `office_id`, `floor_id`, `zone_id`, `magic_number`,`device_type`, `status`, `reinitiate_count`, `device_count`, `parent_created_time`, `parent_updated_time`, `parent_timestamp`) "
                                                       + " SELECT tods.ota_id, tods.process_id, tods.region_id, tods.office_id, tods.floor_id, tods.zone_id, tods.magic_number, tods.device_type, tods.status, tods.reinitiate_count, (SELECT COUNT(*) FROM tbl_devices  AS td WHERE td.zone_id = "+zoneId+" AND td.device_type = "+deviceType+"), tods.created_time, tods.updated_time, tods.timestamp "
                                                       + " FROM tbl_ota_details_set AS tods"
                                                       + " WHERE tods.zone_id="+zoneId
                                                       + " AND tods.device_type="+deviceType+";";



                                        stmt.executeUpdate(sql12);
                                        String sql123 = "DELETE FROM tbl_ota_details_set "

                                                       + " WHERE zone_id="+zoneId+""
                                                       + " AND device_type="+deviceType+";";



                                        stmt.executeUpdate(sql123);
                                        stmt.close();
                                        int magicnum = Integer.parseInt(jedis.get("{OTA_MAGIC_NO}Z" + zoneId).split(":")[0]);
                                        String publishdata = stringbuilder(19, 2) + stringbuilder(5, 4) + stringbuilder(rootOrgId, 4) + stringbuilder(zoneId, 4) + stringbuilder(magicnum, 2);
                                        jedis.publish("complete", rootOrgId + ":" + zoneId + ":" + publishdata);

                                    }
                                    if(DEVICE_VERSION_LOG==1) {
                                        logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"configversion\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"devicetype\":" + deviceType + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"value\":\"" + swversion + ",\"status\":\"" + "sucess" + "\"}";
                                        log.info(logstring);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    log2.fatal(e);
                                    log2.fatal("sql exception in config version update" + e.getMessage());
                                }



                            } else {
                                if(DEVICE_VERSION_LOG==1) {
                                    logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"configversion\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"devicetype\":" + deviceType + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + "subcommand needed(14,15)got " + (decryptedPacket[10] & 0xff) + "\"}";
                                    log.info(logstring);
                                }

                            }
                        } else if (commandByte == Constants.TELINK_DEVICE_VERSION) {
                            String lsb = Integer.toHexString(decryptedPacket[15] & 0xff);
                            String mlsb = Integer.toHexString(decryptedPacket[14] & 0xff);
                            String msb = Integer.toHexString(decryptedPacket[13] & 0xff);
                            String swversion = msb + "." + mlsb + "." + lsb;


                            try {

                                stmt = cmysql.createStatement();
                                String sql = "UPDATE tbl_devices SET sw_version=\"" + swversion + "\" ,timestamp = UNIX_TIMESTAMP(NOW(6)) WHERE  device_id=" + deviceLongId;

                                stmt.addBatch(sql);
                                stmt.executeBatch();
                                stmt.close();

                                stmt = cmysql.createStatement();
                                String sql1 = "SELECT count(*) as count FROM tbl_devices WHERE sw_version!=\"" + swversion + "\" AND zone_id= " + zoneId + " AND device_type= " + deviceType + ";";


                                ResultSet rs = stmt.executeQuery(sql1);

                                int count = 0;
                                while (rs.next()) {
                                    count = rs.getInt("count");
                                }

                                rs.close();

                                stmt.close();
                                if (count == 0) {
                                    stmt = cmysql.createStatement();
                                    String sql12 = "INSERT INTO `tbl_ota_details_set_archive`(`ota_id`, `process_id`, `region_id`, `office_id`, `floor_id`, `zone_id`, `magic_number`,`device_type`, `status`, `reinitiate_count`, `device_count`, `parent_created_time`, `parent_updated_time`, `parent_timestamp`) "
                                                   + " SELECT tods.ota_id, tods.process_id, tods.region_id, tods.office_id, tods.floor_id, tods.zone_id, tods.magic_number, tods.device_type, tods.status, tods.reinitiate_count, (SELECT COUNT(*) FROM tbl_devices  AS td WHERE td.zone_id = "+zoneId+" AND td.device_type = "+deviceType+"), tods.created_time, tods.updated_time, tods.timestamp "
                                                   + " FROM tbl_ota_details_set AS tods"
                                                   + " WHERE tods.zone_id="+zoneId
                                                   + " AND tods.device_type="+deviceType+";";



                                    stmt.executeUpdate(sql12);
                                    String sql123 = "DELETE FROM tbl_ota_details_set"

                                                    + " WHERE zone_id="+zoneId+""
                                                    + " AND device_type="+deviceType+";";



                                    stmt.executeUpdate(sql123);
                                    stmt.close();
                                    int magicnum = Integer.parseInt(jedis.get("{OTA_MAGIC_NO}Z" + zoneId).split(":")[0]);
                                    String publishdata = stringbuilder(19, 2) + stringbuilder(5, 4) + stringbuilder(rootOrgId, 4) + stringbuilder(zoneId, 4) + stringbuilder(magicnum, 2);
                                    jedis.publish("complete", rootOrgId + ":" + zoneId + ":" + publishdata);

                                }
                                if(DEVICE_VERSION_LOG==1) {
                                    logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"telinkversion\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"devicetype\":" + deviceType + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"value\":\"" + swversion + ",\"status\":\"" + "sucess" + "\"}";
                                    log.info(logstring);
                                }


                            } catch (Exception e) {
                                e.printStackTrace();
                                log2.fatal(e);
                                log2.fatal("sql exception in telink version update" + e.getMessage());
                            }



                        } else {
                            if(DEVICE_VERSION_LOG==1) {
                                logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"telinkversion\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"devicetype\":" + deviceType + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + " Command error in  Packet required   command(128,11) got: " + commandByte + "\"}";
                                log.info(logstring);
                            }
                        }


                    } else {
                        if(DEVICE_VERSION_LOG==1) {
                            logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"telinkversion\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + " dzkey not correct got " + dzKey + "\"}";
                            log.info(logstring);
                        }
                    }


                } else {
                    if(DEVICE_VERSION_LOG==1) {
                        logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"telinkversion\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + "crc mismaatch" + "\"}";
                        log.info(logstring);
                    }
                }


            } else {
                if(DEVICE_VERSION_LOG==1) {
                    logstring = "{\"dID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"telinkversion\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + ",\"subCommand\":\"version\",\"status\":\"" + "client cache fail " + clientKey + "\"}";
                    log.info(logstring);
                }
            }


        }catch (Exception e){
            e.printStackTrace();

            log2.fatal("exception in tsdv\n"+e);


        }

    }
    public  String stringbuilder(int num,int length){
        String str=String.format("%0"+(length)+"d",0);

        return (str +Integer.toHexString(num)).substring(Integer.toHexString(num).length());
    }

}
