/**
 * 
 */
package packethandler;


import HelperThreads.*;
import Helperclasses.*;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.SENSOR_LOG;


/**
 * @author arshad
 *
 */
public class SensorTriggerManager implements Runnable{

	static Logger log= Logger.getLogger("sensorLogger");
	static Logger log2 = Logger.getLogger("exceptionLogger");

	CacheCheck cacheCheck=null;

	static HashMap<String,String> bridgeMap;
	static HashMap<String,String> deviceMap;
	static HashMap<String,String> alertMap;

	BlockingQueue<RedisPojo> queue;
	BlockingQueue<String> pgsqlqueue;

	byte[] packet=null;
	long starttime;


	public SensorTriggerManager(byte[] packet,long starttime) {
		this.packet=packet;
		this.starttime=starttime;
		queue= RedisThreads.getinstance().RedisdataQueue;
		pgsqlqueue= PgsqlThreads.getinstance().PgsqldataQueue;
		cacheCheck =CacheCheck.getInstance();
	}





	public void run() {
		try {


			StringBuffer result4 = new StringBuffer();
			for (byte b : packet) {
				result4.append(String.format("%02X ", b));
				result4.append(" "); // delimiter
			}
			long Pkttimestamp=System.currentTimeMillis()/1000;

			if(packet.length==32){
				Pkttimestamp=((packet[28] & 0xff) << 24 | (packet[29] & 0xff) << 16| (packet[30] & 0xff) << 8 | (packet[31] & 0xff));
				packet= Arrays.copyOfRange(packet,0,28);
			}

			bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
			deviceMap= Deviceverifier.getinstance().FullDeviceMap;
			alertMap= AlertSettingThread.getinstance().FullAlertMap;

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
			String deviceType = "";

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

					log2.fatal(e);
					log2.fatal("decrypt exception in tsst");

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
						deviceType = dvc[4];



						redisKey = "{LRC}" + "DI" + deviceLongId;
						redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + timeStamp + ":1";

						queue.put(new RedisPojo(redisKey,redisData));

						int commandByte = decryptedPacket[8] & 0xff;
						if (Maps.getInstance().sequencenumber((long) deviceLongId,sequenceNum)) {
							if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {
								int statusId = decryptedPacket[9] & 127;

								long timeSt = System.currentTimeMillis() / 1000;
								Long time = System.currentTimeMillis() / 1000;
								time = time * 1000000 + (LocalDateTime.now().getLong(ChronoField.MICRO_OF_SECOND));
								String key = "{SR}" + wsId;



								double humidity = 0;
								double temperature = 0;
								int batterylevel = 0;

								String val = "";
								String sql="";

								switch (statusId) {


									case Constants.SENSOR_STATUS:





										double humiditypresent = ((decryptedPacket[10] >> 2) & 1);

										if (humiditypresent == 1) {
											humidity = (((decryptedPacket[12] & 0x3f) << 8) | (decryptedPacket[11] & 0xff)) << 2;

											humidity = ((125 * humidity) / 65536) - 6;
											humidity = BigDecimal.valueOf(humidity).setScale(1, RoundingMode.HALF_UP).doubleValue();
										} else humidity = 0;

										double temperaturepresent = ((decryptedPacket[10] >> 1) & 1);

										if (temperaturepresent == 1) {

											temperature = (((decryptedPacket[14] & 0x3f) << 8) | (decryptedPacket[13] & 0xff)) << 2;

											temperature = (((175.72 * temperature) / 65536) - 46.85);
											temperature = BigDecimal.valueOf(temperature).setScale(1, RoundingMode.HALF_UP).doubleValue();
											Double a = 1.8;
											Double temp_in_cel = temperature;
											temperature = (temperature * a) + 32;

											int alertType = 8;
											String jedisKey = "{ASC}AT" + alertType + "RO" + rootOrgId;
											if (alertMap.containsKey(jedisKey)) {
												String[] vals = alertMap.get(jedisKey).split(":");
												double alertLevel = Double.parseDouble(vals[2]);
												if (temp_in_cel > alertLevel) {

//
													int category = 2;
													int customerId = 1;

													double range = temperature;


													String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;
//
													String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
															regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + deviceLongId + ":" + deviceType + ":" + range + ":" + String.valueOf(time);

													queue.put(new RedisPojo(alertKey,alertVal));
												}
											}
										} else {
//
											temperature = 0;
//
										}


										double batterypresent = ((decryptedPacket[10] & 0xff) & 1);

										if (batterypresent == 1) {
											batterylevel = decryptedPacket[15] & 0xff;
											int alertType = 4;
											String jedisKey = "{ASC}AT" + alertType + "RO" + rootOrgId;

											if (alertMap.containsKey(jedisKey)) {
												String[] vals = alertMap.get(jedisKey).split(":");
												double alertLevel = (Double.parseDouble(vals[2]) * 7) / 100;
												if (batterylevel < alertLevel) {

//

													int category = 2;
													int customerId = 1;

													int range = (batterylevel*100)/7;


													String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;
//
													String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
															regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + deviceLongId + ":" + deviceType + ":" + range + ":" +time;

													queue.put(new RedisPojo(alertKey,alertVal));
												}
											}
										} else {
//
											batterylevel = 0;
//
										}


										String sqlVal1 = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + Pkttimestamp + ":" + temperature + ":" + humidity + ":" + 0 + ":" + batterylevel + ":" + sequenceNum;
										String[] value =sqlVal1.split(":");
										sql= "INSERT INTO wiseconnect.tbl_sensor_data "
												+ "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,timestamp,temperature,humidity,presence,battery, nfc,seq) VALUES ("
												+ value[0] + "," + value[1] + "," + value[2] + "," + value[3] + "," + value[4] + ","
												+ value[5] + "," + value[6] + "," + value[7] + "," + value[8] + "," + 0 + ","
												+ value[10] + "," + 0 + "," + value[11] +")";

										pgsqlqueue.put(sql);


										val = humidity + ":" + temperature + ":" + batterylevel + ":" + 0 + ":" + System.currentTimeMillis() / 1000;

										queue.put(new RedisPojo(key,val));
										if(SENSOR_LOG==1) {
											logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"temp\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"value\":" + temperature + ",\"status\":" + "\"sucess\"" + "}";
											log.info(logstring);
											logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"humidity\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"value\":" + humidity + ",\"status\":" + "\"sucess\"" + "}";
											log.info(logstring);
											logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"battery\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"value\":" + batterylevel + ",\"status\":" + "\"sucess\"" + "}";
											log.info(logstring);
										}
										break;

									case Constants.MOTION_REPORT:





										 int motionReport = decryptedPacket[15] & 0xff;
										int report = motionReport;

										String sqlVal2 = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + Pkttimestamp + ":" + motionReport+ ":" + sequenceNum;
										String[] value1=sqlVal2.split(":");
										sql = "INSERT INTO wiseconnect.tbl_occupancy "
												+ "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,timestamp,duration,seq) VALUES ("
												+ value1[0] + "," + value1[1] + "," + value1[2] + "," + value1[3] + "," + value1[4] + ","
												+ value1[5] + "," + value1[6] + "," + value1[7] + "," + value1[8]+")";

										pgsqlqueue.put(sql);

										String sqlVal = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + timeSt + ":0:" + motionReport;

										String sqlKey = "{OCCUPANCY}WI" + wsId;
//										System.out.print(motionReport + "----report");



										queue.put(new RedisPojo(sqlKey,sqlVal));

										if(SENSOR_LOG==1) {
											logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"motion\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"value\":" + report + ",\"status\":" + "\"sucess\"" + "}";
											log.info(logstring);
										}


										break;

								}


							} else {
								if(SENSOR_LOG==1) {
									logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"---\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "\"another command\"" + "}";
									log.info(logstring);
								}


							}


						} else {
							if(SENSOR_LOG==1) {
								logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"---\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "\"sequenceno mismatch from packet: " + sequenceNum + "\"" + "}";
								log.info(logstring);
							}

						}
					} else {
						if(SENSOR_LOG==1) {
							logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"---\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\" " + ",\"status\":" + "\"device long id not found key from packet: " + dzKey + "\"" + "}";
							log.info(logstring);
						}
						String[] arr={"3","0",""+deviceShortId,""+zoneId};
						cacheCheck.insertData(arr);


					}

				} else {


					if(SENSOR_LOG==1) {
						logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"---\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\" " + ",\"status\":" + "\"crc mismatch\"" + "}";
						log.info(logstring);
					}

				}

			} else {


				if(SENSOR_LOG==1) {
					logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"---\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"status\":" + "\"clientkey mismatch" + clientKey + "\"" + "}";
					log.info(logstring);
				}

				String[] arr={"1",bridgeId+"","0","0"};
				cacheCheck.insertData(arr);



			}
		}catch (Exception e){

			e.printStackTrace();

			log2.fatal("exception in tsst parsing\n"+e);


		}
	}
}

