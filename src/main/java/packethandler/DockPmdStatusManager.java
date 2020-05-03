
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

import static Helperclasses.Constants.STATUS_LOG;


/**
 * @author arshad
 *
 */
public class DockPmdStatusManager implements Runnable {

	static Logger log = Logger.getLogger("statusLogger");
	static Logger log2 = Logger.getLogger("exceptionLogger");

	static HashMap<String,String> bridgeMap;
	static HashMap<String,String> deviceMap;
	static HashMap<String,String> alertMap;





	CacheCheck cacheCheck=null;

	BlockingQueue<RedisPojo> queue;
	BlockingQueue<String> pgsqlqueue;
	BlockingQueue<String> mysqlqueue;

	byte[] packet=null;
	long starttime;

	public DockPmdStatusManager(byte[] packet,long starttime ) {
		this.packet=packet;
		this.starttime=starttime;
		queue= RedisThreads.getinstance().RedisdataQueue;
		pgsqlqueue= PgsqlThreads.getinstance().PgsqldataQueue;
		mysqlqueue= MysqlThreads.getinstance().MysqldataQueue;
		cacheCheck =CacheCheck.getInstance();
	}







	private String[] queryBuilder(String sql, int id, int status, String [] det, long currtime){

		String query = "";
		


		int prevstatus = Integer.parseInt(det[1]);
		long prevtime = Integer.parseInt(det[2]);
		long diff = currtime - prevtime;

		if(prevstatus == 0 && status == 0) {
			query = sql + id + ",0,0,"+ currtime + "," +0+")";
			diff = 0;
		}else if(prevstatus == 0 && status == 1) {
			query = sql + id + ",0,0,"+ currtime + ","+0+")";
			diff = 0;

		}else if(prevstatus == 1 && status == 0) {
			query = sql + id + ",0,0,"+ currtime  + "," +diff+")";

		}else if(prevstatus == 1 && status == 1 ) {
			query = sql + id + ",0,0,"+ currtime + "," + diff+")";
		}
		
		return new String[] {String.valueOf(diff), query};
		
		//return  query;

		}



		public void run() {
			try {
				long Pkttimestamp=System.currentTimeMillis() / 1000;

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
				alertMap= AlertSettingThread.getinstance().FullAlertMap;










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



							if (Maps.getInstance().sequencenumber((long) deviceLongId,sequenceNum)) {
								if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {

									int thermalstatus = 0;
									int thermalshutdownstatus = 0;
									int temperature = 0;

									Long time = System.currentTimeMillis() / 1000;
									time = time * 1000000 + (LocalDateTime.now().getLong(ChronoField.MICRO_OF_SECOND));



									switch (statusId) {
										case Constants.DOCK_STATUS:

											int sbcCopernicus = ((decryptedPacket[15] >> 5) & 1);


											int status = ((decryptedPacket[15] >> 4) & 1);

											int dockstatus = ((decryptedPacket[15] >> 1) & 1);


											int resetrequired = 0;
											int resetstatus = 0;

											thermalstatus = ((decryptedPacket[15] & 0xff) & 1);
											thermalshutdownstatus = ((decryptedPacket[15] & 0xff) & 8);
											int overcurrentstatus = ((decryptedPacket[15] >> 2) & 1);


											temperature = (((decryptedPacket[11] & 0xff) << 8) | (decryptedPacket[10] & 0xff));

											double temper = (((175.72 * temperature) / 65536) - 46.85);
											temper = BigDecimal.valueOf(temper).setScale(1, RoundingMode.HALF_UP).doubleValue();
											temperature = (int) temper;
											temperature = (int) ((temperature * 1.8D) + 32);

											if(alertMap.containsKey("{ASC}AT16RO"+rootOrgId)){
												if(Float.parseFloat(alertMap.get("{ASC}AT16RO"+rootOrgId).split(":")[2])<(float)temper){

													int category = 2;
													int alertType = 16;
													int customerId = 1;

													int range = temperature;


													String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;




													String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
															regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" +  deviceLongId+ ":" + deviceType + ":" + range + ":" +time;

													queue.put(new RedisPojo(alertKey,alertVal));
												}
											}


											if (thermalstatus == 1) {
												String tempKey = "{ITC}DI" + deviceLongId;

												String tempValue = deviceLongId + ":" + temperature + ":" + timeStamp;

											}
											if (thermalshutdownstatus == 8) {

												queue.put(new RedisPojo("{THERMAL}DL" + deviceLongId,"1"));


												int category = 1;
												int alertType = 2;
												int customerId = 1;

												int range = temperature;


												String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;



												String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
														regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + deviceLongId  + ":" +  deviceType + ":" + range + ":" + time;

												queue.put(new RedisPojo(alertKey,alertVal));

											} else {

												queue.put(new RedisPojo("{THERMAL}DL" + deviceLongId,"0"));
											}

											if (overcurrentstatus == 1) {
												String overcuKey = "{OCS}DI" + deviceLongId;
												String overcuValue = deviceLongId + ":" + "0" + ":" + "1" + ":" + "0" + ":" + "0" + ":" + "0" + ":" + timeStamp;

												queue.put(new RedisPojo(overcuKey,overcuValue));
											}


											int dlchip = 0;
											int dbupdate=1;
											if (sbcCopernicus == 1) {


												int port1 = ((decryptedPacket[13] >> 5) & 1);
												int port2 = ((decryptedPacket[13] >> 4) & 1);
												int port3 = ((decryptedPacket[13] >> 3) & 1);


												//port 4 use as hub2 status
												int port4 = 0;
												int port5 = 0;


												int usbhub = 0;
												if (port1 == 0) {
													//USB host connected on S4, S5 mode
													usbhub = 0;
												} else if (port1 == 1) {
													//USB host other working modes
													usbhub = 1;
												}

												//hub2 status
												String hubMes = "";
												if (port1 == 0 && port2 == 0) {
													hubMes = "No USB host connected";
													port4 = 0;
												} else if (port1 == 0 && port2 == 1) {
													hubMes = "Error message";
													port4 = 1;
												} else if (port1 == 1 && port2 == 0) {
													hubMes = "SB host connected in S4";
													port4 = 2;
												} else if (port1 == 1 && port2 == 0) {
													hubMes = "USB Host other working modes";
													port4 = 3;
												}


												dlchip = ((decryptedPacket[14] >> 3) & 1);

												int audioIn = ((decryptedPacket[13] >> 1) & 1);
												int audioOut = ((decryptedPacket[13] & 0xff) & 1);

												int test = 0;

												if (dlchip == 0 || (port2 == 0)) {
													test = 0;
												} else {
													test = 1;
												}


												int audio = 0;

												if (audioIn == 1 || audioOut == 1) {
													audio = 1;
												} else {
													audio = 0;
												}


												int laptopdocked = ((decryptedPacket[13] >> 2) & 1);


												int video1 = ((decryptedPacket[14] >> 5) & 1);


												int video2 = ((decryptedPacket[14] >> 7) & 1);


												int video3 = ((decryptedPacket[14] >> 4) & 1);


												int video4 = ((decryptedPacket[14] >> 6) & 1);


												int es1 = ((decryptedPacket[14] >> 2) & 1);
												int es2 = ((decryptedPacket[14] >> 1) & 1);
												int es3 = ((decryptedPacket[14] & 0xff) & 1);

												String ethernet = String.valueOf(es1) + String.valueOf(es2) + String.valueOf(es3);

												if (es1 == 0 && es2 == 0 && es3 == 0) {
													es1 = 0;
												} else {
													es1 = 1;
												}


												String ethernetSpeed = "0";


												if (ethernet.equals("000")) {
													ethernetSpeed = "0";
												} else if (ethernet.equals("001")) {
													ethernetSpeed = "1";
												} else if (ethernet.equals("011")) {
													ethernetSpeed = "2";
												} else if (ethernet.equals("111")) {
													ethernetSpeed = "3";
												}


												String portId1 = dvc[12];
												String portId2 = dvc[13];
												String portId3 = dvc[14];
												String portId4 = dvc[15];
												String portId5 = dvc[16];
												String videoId1 = dvc[17];
												String videoId2 = dvc[18];
												String videoId3 = dvc[19];
												String videoId4 = dvc[20];
												String ethernetId = dvc[21];
												String usbHubPortId = dvc[22];


												String dlChipId = dvc[23];
												String laptopDockId = dvc[24];
												String audioId = dvc[25];


												String dockStatval = "";

												String dockStatusKey = "{DS}Z" + zoneId + "D" + deviceLongId;
												dockStatval = Maps.getInstance().getStatus(dockStatusKey);
												//postgres insert
												String sql = "INSERT INTO wiseconnect.tbl_port_usage "
														+ "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,seq,port_id,pid,vid,timestamp,duration) VALUES ("
														+ rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId + ","+sequenceNum+",";


												int[] duration = new int[14];

												if (!dockStatval.equals("")) {

													String[] que = new String[2];

													String[] dockPrevStat = dockStatval.split(":");
													String[] portId1Det = dockPrevStat[0].split("-");
													String[] portId2Det = dockPrevStat[1].split("-");
													String[] portId3Det = dockPrevStat[2].split("-");
													String[] portId4Det = dockPrevStat[3].split("-");
													String[] portId5Det = dockPrevStat[4].split("-");
													String[] video1Det = dockPrevStat[5].split("-");
													String[] video2Det = dockPrevStat[6].split("-");
													String[] video3Det = dockPrevStat[7].split("-");
													String[] video4Det = dockPrevStat[8].split("-");
													String[] ethernetDet = dockPrevStat[9].split("-");
													String[] usbhubDet = dockPrevStat[10].split("-");
													String[] lapdockDet = dockPrevStat[11].split("-");
													String[] audioDet = dockPrevStat[12].split("-");
													String[] dlchipDet = dockPrevStat[13].split("-");

													int flag = 0;


													int prevStatus = Integer.parseInt(dockPrevStat[14]);
													int prevThStatus = Integer.parseInt(dockPrevStat[17]);
													int prevOcStatus = Integer.parseInt(dockPrevStat[18]);
													int prevTemp = Integer.parseInt(dockPrevStat[20]);
													String prevEthSpeed = ethernetDet[4];
													long prevTimeSt = Long.parseLong(dockPrevStat[21]);


													flag = 1;



													if (flag == 1) {

														try {


															try {



																que = queryBuilder(sql, Integer.parseInt(portId1), port1, portId1Det, Pkttimestamp);

																duration[0] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(portId2), port2, portId2Det, Pkttimestamp);


																duration[1] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(portId3), port3, portId3Det, Pkttimestamp);


																duration[2] = Integer.parseInt(que[0]);


																que = queryBuilder(sql, Integer.parseInt(portId4), port4, portId4Det, Pkttimestamp);


																duration[3] = Integer.parseInt(que[0]);


																que = queryBuilder(sql, Integer.parseInt(portId5), port5, portId5Det, Pkttimestamp);


																pgsqlqueue.put(que[1]);
																duration[4] = Integer.parseInt(que[0]);


																que = queryBuilder(sql, Integer.parseInt(videoId1), video1, video1Det, Pkttimestamp);

																pgsqlqueue.put(que[1]);																duration[5] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(videoId2), video2, video2Det, Pkttimestamp);

																pgsqlqueue.put(que[1]);																duration[6] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(videoId3), video3, video3Det, Pkttimestamp);

																pgsqlqueue.put(que[1]);
																duration[7] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(videoId4), video4, video4Det, Pkttimestamp);

																pgsqlqueue.put(que[1]);
																duration[8] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(ethernetId), es1, ethernetDet, Pkttimestamp);

																pgsqlqueue.put(que[1]);
																duration[9] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(usbHubPortId), usbhub, usbhubDet, Pkttimestamp);
																pgsqlqueue.put(que[1]);
																duration[10] = Integer.parseInt(que[0]);


																que = queryBuilder(sql, Integer.parseInt(laptopDockId), laptopdocked, lapdockDet, Pkttimestamp);
																pgsqlqueue.put(que[1]);
																duration[11] = Integer.parseInt(que[0]);


																String sqldur = "INSERT INTO wiseconnect.tbl_usage_duration "
																		+ "(root_org_id,region_id,office_id,floor_id,zone_id,workstation_id,seq,timestamp,duration) VALUES ("
																		+ rootOrgId + "," + regionId + "," + officeId + "," + floorId + "," + zoneId + "," + wsId + ","+sequenceNum+ "," + Pkttimestamp + "," + duration[11] + ")";

																pgsqlqueue.put(que[1]);

																que = queryBuilder(sql, Integer.parseInt(audioId), audio, audioDet, Pkttimestamp);
																pgsqlqueue.put(que[1]);
																duration[12] = Integer.parseInt(que[0]);

																que = queryBuilder(sql, Integer.parseInt(dlChipId), dlchip, dlchipDet, Pkttimestamp);
																pgsqlqueue.put(que[1]);
																duration[13] = Integer.parseInt(que[0]);



															} catch (Exception e) {
																log2.fatal(e);
																log2.info("post excetion in tsts");
															}


														} catch (NumberFormatException e) {

															log2.fatal(e);
															log2.fatal("number format exception in tsts");



														}
														int update=1;



															String[] sqlVal = {deviceLongId+"", zoneId+"", wsId, bridgeLongId+"", temperature+"", ethernetSpeed, thermalstatus+"", overcurrentstatus+"", port1+"", port2+"", port3+"", port4+"", port5+"", video1+"", video2+"", video3+"", video4+"", es1+"", usbhub+"", laptopdocked+"", audio+"", dlchip+"", status+"", update+""};
															MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("dockstatus",sqlVal));



													}


												} else {

													int update=1;


													String[] sqlVal = {deviceLongId+"", zoneId+"", wsId, bridgeLongId+"", temperature+"",ethernetSpeed, thermalstatus+"", overcurrentstatus+"", port1+"", port2+"", port3+"", port4+"", port5+"", video1+"", video2+"", video3+"", video4+"", es1+"", usbhub+"", laptopdocked+"", audio+"", dlchip+"", status+"", update+""};
													MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("dockstatus",sqlVal));



												}


												dockStatval = portId1 + '-' + port1 + '-' + Pkttimestamp + '-' + duration[0] + ":" +
														portId2 + '-' + port2 + '-' + Pkttimestamp + '-' + duration[1] + ":" +
														portId3 + '-' + port3 + '-' + Pkttimestamp + '-' + duration[2] + ":" +
														portId4 + '-' + port4 + '-' + Pkttimestamp + '-' + duration[3] + ":" +
														portId5 + '-' + port5 + '-' + Pkttimestamp + '-' + duration[4] + ":" +
														videoId1 + '-' + video1 + '-' + Pkttimestamp + '-' + duration[5] + ":" +
														videoId2 + '-' + video2 + '-' + Pkttimestamp + '-' + duration[6] + ":" +
														videoId3 + '-' + video3 + '-' + Pkttimestamp + '-' + duration[7] + ":" +
														videoId4 + '-' + video4 + '-' + Pkttimestamp + '-' + duration[8] + ":" +
														//ethernetId+'-'+es1+'-'+timeSt + '-' + ethernetSpeed +  '-' + duration[9] + ":" +
														ethernetId + '-' + es1 + '-' + Pkttimestamp + '-' + duration[9] + '-' + ethernetSpeed + ":" +
														usbHubPortId + '-' + usbhub + '-' + Pkttimestamp + '-' + duration[10] + ":" +
														laptopDockId + '-' + laptopdocked + '-' + Pkttimestamp + '-' + duration[11] + ":" +
														audioId + '-' + audio + '-' + Pkttimestamp + '-' + duration[12] + ":" +
														dlChipId + '-' + dlchip + '-' + Pkttimestamp + '-' + duration[13] + ":" +
														status + ":" +
														resetrequired + ":" +
														resetstatus + ":" +
														thermalstatus + ":" +
														overcurrentstatus + ":" +
														dockstatus + ":" +
														temperature + ":" +
														Pkttimestamp + ":" +
														wsId + ":" + test;


												queue.put(new RedisPojo(dockStatusKey,dockStatval));
												Maps.getInstance().setStatus(dockStatusKey,dockStatval);


											} else {


												String portId1 = dvc[12];



												int update=1;

												String[] sqlVal = {deviceLongId+"", zoneId+"", wsId, bridgeLongId+"", temperature+"", thermalstatus+"", overcurrentstatus+"", portId1, status+"", update+""};
												MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("dockstatus",sqlVal));







											}
											if(STATUS_LOG==1) {

												log.info("{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis() + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+",\"type\":13002,\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"seqNo\":" + sequenceNum + ",\"temperature\":" + temperature + ",\"status\":\"success\"}");

											}
											break;

										case Constants.PMD_STATUS:
											thermalstatus = ((decryptedPacket[10] & 0xff) & 1);
											int ocpmdport4 = ((decryptedPacket[11] >> 3) & 1);
											int ocpmdport3 = ((decryptedPacket[11] >> 2) & 1);
											int ocpmdport2 = ((decryptedPacket[11] >> 1) & 1);
											int ocpmdport1 = ((decryptedPacket[11] & 0xff) & 1);


											String portId1 = dvc[12];
											String portId2 = dvc[13];
											String portId3 = dvc[14];
											String portId4 = dvc[15];


											if (ocpmdport1 == 1 || ocpmdport2 == 1 || ocpmdport3 == 1 || ocpmdport4 == 1) {

												String overcurrentKey = "{OCS}DI" + deviceLongId;

												String overcurrentVal = deviceLongId + ":" + "0" + ":" + ocpmdport1 + ":" + ocpmdport2 + ":" + ocpmdport3 + ":" + ocpmdport4 + ":" + timeStamp;


												queue.put(new RedisPojo(overcurrentKey,overcurrentVal));
											}


											temperature = (((decryptedPacket[13] & 0x3f) << 8) | (decryptedPacket[12] & 0xff)) << 2;
											double tempert = (((175.72 * temperature) / 65536) - 46.85);
											tempert = BigDecimal.valueOf(tempert).setScale(1, RoundingMode.HALF_UP).doubleValue();
											temperature = (int) tempert;
											temperature = (int) ((temperature * 1.8D) + 32);

											if(alertMap.containsKey("{ASC}AT16RO"+rootOrgId)){
												if(Float.parseFloat(alertMap.get("{ASC}AT16RO"+rootOrgId).split(":")[2])<(float)tempert){

													int category = 2;
													int alertType = 16;
													int customerId = 1;

													int range = temperature;


													String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;




													String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
															regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" +  deviceLongId + ":" + deviceType  + ":" + range + ":" + String.valueOf(time);

													queue.put(new RedisPojo(alertKey,alertVal));
												}
											}


											String tempKey = "{ITC}DI" + deviceLongId;

											String tempValue = deviceLongId + ":" + temperature + ":" + timeStamp;
											queue.put(new RedisPojo(tempKey,tempValue));
											if (thermalstatus == 1) {

												queue.put(new RedisPojo("{THERMAL}DL" + deviceLongId,"1"));


												int category = 1;
												int alertType = 2;
												int customerId = 1;
												int range = temperature;


												String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId + "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;



												String alertVal = category + ":" + alertType + ":" + customerId + ":" + rootOrgId + ":" +
														regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" +  deviceLongId + ":" + deviceType + ":" + range + ":" + time;


												queue.put(new RedisPojo(alertKey,alertVal));

											} else {

												queue.put(new RedisPojo("{THERMAL}DL" + deviceLongId,"0"));
											}

											int gpo4 = ((decryptedPacket[15] >> 3) & 1);
											int gpo3 = ((decryptedPacket[15] >> 2) & 1);
											int gpo2 = ((decryptedPacket[15] >> 1) & 1);
											int gpo1 = ((decryptedPacket[15] & 0xff) & 1);



											String[] spUpdate = {deviceLongId+"", wsId, temperature+"", gpo1+"", gpo2+"", gpo3+"", gpo4+"", ocpmdport1+"", ocpmdport2+"", ocpmdport3+"", ocpmdport4+"", thermalstatus+""};
											MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("pmdstatus",spUpdate));






											if(STATUS_LOG==1) {

												log.info("{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis()+ ",\"taotaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+ ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"seqNo\":" + sequenceNum + ",\"temperature\":" + temperature + ",\"status\":\"success\"}");


											}

											break;


									}
								} else {
									if(STATUS_LOG==1) {
										log.info("{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"seqNo\":" + sequenceNum + ",\"status\":\"anothercommand\"}");
									}
								}

							} else {
								if(STATUS_LOG==1) {
									log.info("{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+  ",\"first\":"+(starttime)+",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"seqNo\":" + sequenceNum + ",\"status\":\"sequence number check failed\"}");
								}
							}
						} else {
							if(STATUS_LOG==1) {
								log.info("{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime)+",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"seqNo\":" + sequenceNum + ",\"status\":\"dzkey notfounr" + dzKey + "\"}");
							}
							String[] arr={"3","0",""+deviceShortId,""+zoneId};
							cacheCheck.insertData(arr);

//
						}

					} else {
						if(STATUS_LOG==1) {
							log.info("{\"dID\":" + deviceLongId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis()  + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+ ",\"first\":"+(starttime)+",\"timefrompacket\":" + Pkttimestamp+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + " \",\"dpkt\":\"" + result3.toString() + " \",\"status\":\"crc failed\"}");
						}
					}

				} else {
					if(STATUS_LOG==1) {
						log.info("{\"dID\":" + deviceLongId + ",\"pkttype\":\"status\",\"epoch\":" + System.currentTimeMillis() + ",\"totaltime\":"+(System.currentTimeMillis()-starttime)+ ",\"first\":"+(starttime)+",\"timefrompacket\":" + Pkttimestamp+",\"zId\":" + zoneId + ",\"bId\":" + bridgeLongId + ",\"pkt\":\"" + result4.toString() + ",\"status\":\"clienykey not found" + clientKey + "\"}");
					}
					String[] arr={"1",bridgeId+"","0","0"};
					cacheCheck.insertData(arr);

				}

			} catch (Exception e){
				e.printStackTrace();
				log2.fatal("exception in tsts parsing\n"+e);
			}
		}
	}

