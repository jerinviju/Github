/**
 * 
 */
package packethandler;


import Connectionclasses.RedisDbFactoryClient;
import HelperThreads.AlertSettingThread;
import HelperThreads.Bridgeverifier;
import HelperThreads.Deviceverifier;
import HelperThreads.RedisThreads;
import Helperclasses.*;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.RFID_LOG;


/**
 * @author arshad
 *
 */
public class RFIDManager implements Runnable{

	static Logger log= Logger.getLogger("rfidLogger");
	static Logger log2 = Logger.getLogger("exceptionLogger");

	static HashMap<String,String> bridgeMap;
	static HashMap<String,String> deviceMap;
	static HashMap<String,String> alertMap;

	BlockingQueue<RedisPojo> queue;

	byte[] packet=null;
	long starttime;

    public RFIDManager(byte[] packet,long starttime) {
		this.packet=packet;
		this.starttime=starttime;
		queue= RedisThreads.getinstance().RedisdataQueue;


	}





	public void run() {

		try {

			long Pkttimestamp=0l;
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
			String networkId = "";

			long timeStamp = System.currentTimeMillis() / 1000;


			String clientKey = "{CLIENT_CACHE}CI" + bridgeId;
			if (bridgeMap.containsKey(clientKey)) {
				String[] userd = bridgeMap.get(clientKey).split(":");
				bridgeLongId = Integer.parseInt(userd[0]);
				networkId = userd[3];
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
					log2.fatal("decrypt error in tsrd");

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

						int seq = 0;




						String regionId = dvc[8];
						String officeId = dvc[9];
						String floorId = dvc[10];
						String wsId = dvc[11];
						String deviceType = dvc[4];

						if (seq < sequenceNum) {
							redisKey = "{LRC}" + "DI" + deviceLongId;
							redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId
									+ ":" + timeStamp + ":1";
							queue.put(new RedisPojo(redisKey,redisData));

							int commandByte = decryptedPacket[8] & 0xff;
							if (commandByte == Constants.STATUS_DOCKPMD_COMMAND) {
								int statusId = decryptedPacket[9] & 127;

								Long time = System.currentTimeMillis() / 1000;
								time = time * 1000000 + (LocalDateTime.now().getLong(ChronoField.MICRO_OF_SECOND));
								switch (statusId) {


									case Constants.RFID_STATUS:

										if (decryptedPacket[10] == 2) {
											Thread.sleep(3);
										}else{

										}

										String rfidTempKey = "{TRF}D" + deviceLongId;

										boolean successflag = false;

										String rfidPrevData = Maps.getInstance().getRfid(rfidTempKey);
										long uuidDet = 0l;
										if (!rfidPrevData.equals("")) {
											Thread.sleep(150);
											successflag = true;
											String[] rfidPrevTemp = rfidPrevData.split(":");
											if (decryptedPacket[10] == 2) {


												int uuid1 = Integer.parseInt(rfidPrevTemp[1]);
												int uuid2 = Integer.parseInt(rfidPrevTemp[2]);
												int uuid3 = Integer.parseInt(rfidPrevTemp[3]);
												int uuid4 = Integer.parseInt(rfidPrevTemp[4]);


												int uuid5 = decryptedPacket[11];
												int uuid6 = decryptedPacket[12];
												int uuid7 = decryptedPacket[13];

												uuidDet = (
														(uuid7 & 0xff) << 128 | (uuid6 & 0xff) << 64 | (uuid5 & 0xff) << 32
																| (uuid4 & 0xff) << 24 | (uuid3 & 0xff) << 16 | (uuid2 & 0xff) << 8
																| (uuid1 & 0xff)
												);




											} else if (decryptedPacket[10] == 1) {

												int uuid1 = decryptedPacket[12];
												int uuid2 = decryptedPacket[13];
												int uuid3 = decryptedPacket[14];
												int uuid4 = decryptedPacket[15];


												int uuid5 = Integer.parseInt(rfidPrevTemp[1]);
												int uuid6 = Integer.parseInt(rfidPrevTemp[2]);
												int uuid7 = Integer.parseInt(rfidPrevTemp[3]);


												uuidDet = (
														(uuid7 & 0xff) << 128 | (uuid6 & 0xff) << 64 | (uuid5 & 0xff) << 32
																| (uuid4 & 0xff) << 24 | (uuid3 & 0xff) << 16 | (uuid2 & 0xff) << 8
																| (uuid1 & 0xff)
												);
											}


										} else {
											successflag = false;
											String val = "";
											if (decryptedPacket[10] == 2) {
												int uuid5 = decryptedPacket[11];
												int uuid6 = decryptedPacket[12];
												int uuid7 = decryptedPacket[13];

												val = deviceLongId + ":" + uuid5 + ":" + uuid6 + ":" + uuid7 + ":0";

												Maps.getInstance().setRfid(rfidTempKey,val);


											} else {
												int uuid1 = decryptedPacket[12];
												int uuid2 = decryptedPacket[13];
												int uuid3 = decryptedPacket[14];
												int uuid4 = decryptedPacket[15];

												val = deviceLongId + ":" + uuid1 + ":" + uuid2 + ":" + uuid3 + ":" + uuid4;

												Maps.getInstance().setRfid(rfidTempKey,val);

											}
											if(RFID_LOG==1) {
												logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"rfidPktNo\":2,\"rfidValue\":" + uuidDet + ",\"status\":" + "first packet" + "}";
												log.info(logstring);
											}
										}

										if (successflag) {


											String Nfckey = "{NFCWS}WI"+wsId;



											Maps.getInstance().deleteRfid(rfidTempKey);



											String rfidKey = "{RF}" + uuidDet;

											Jedis jedis= RedisDbFactoryClient.getInstance().getConnection();

											if (jedis.exists(rfidKey)) {
												String  str=rfidKey+":";
												String val=jedis.get(rfidKey);
												str =str+val;
												String[] rfcheck = val.split(":");

												boolean flag = false;
												switch (rfcheck[1]) {
													case "0":
														str=str+":0";
														if (Arrays.asList(rfcheck[2].split("-")).contains(regionId + "")) {
															flag = true;
															str=str+":1";
														}
														str=str+"2";
														break;
													case "1":
														str=str+":4";
														if (Arrays.asList(rfcheck[2].split("-")).contains(officeId + "")) {
															flag = true;
															str=str+":3";
														}

														break;
													case "2":
														str=str+":6";
														if (Arrays.asList(rfcheck[2].split("-")).contains(floorId + "")) {
															flag = true;
															str=str+":5";
														}

														break;

													case "3":
														str=str+":8";
														if (Arrays.asList(rfcheck[2].split("-")).contains(zoneId + "")) {
															flag = true;
															str=str+":7";
														}

														break;
													case "4":
														str=str+":10";
														if (Arrays.asList(rfcheck[2].split("-")).contains(wsId + "")) {
															flag = true;
															str=str+":9";
														}

														break;
													case "-1":
														str=str+":11";
														flag = false;
														break;
												}



												boolean newflag=true;
												if (jedis.exists(Nfckey)) {
													str=str+":12:"+Nfckey;
													String data1 = jedis.get(Nfckey);
													if (data1.equals("1")) {

														str=str+":13:"+data1;
														newflag = false;

													}
												}

												if(newflag) {

													if (!flag) {

														int category = 2;
														int alertType = 7;
														int customerId = 1;


														if (RFID_LOG == 1) {
															logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() + ",\"timefrompacket\":" + Pkttimestamp + ",\"first\":" + (starttime - System.currentTimeMillis()) + ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"rfidPktNo\":2,\"rfidValue\":" + uuidDet + ",\"status\":" + "unautherized" + str + "}";
															log.info(logstring);
														}

														String alertKey = "{AC}CG" + category + "AT" + alertType + "CI" + customerId
																+ "RO" + rootOrgId + "RI" + regionId + "OI" + officeId + "FI"
																+ floorId + "ZI" + zoneId + "WI" + wsId + "DI" + deviceLongId;

														String wcval = jedis.get("{WC}RI" + regionId + "OI" + officeId + "FI" + floorId + "ZI" + zoneId + "WI" + wsId);
														String alertVal = category + ":" + alertType + ":" + customerId + ":"
																+ rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":"
																+ zoneId + ":" + wsId + ":" + deviceLongId + ":" + deviceType + ":"
																+ wcval.split(":")[5] + ":" + String.valueOf(time);


														queue.put(new RedisPojo(alertKey,alertVal));

													} else {

														String authKey = "{AUTH}" + uuidDet;
														String userId = jedis.get(rfidKey).split(":")[0];
														String authVal = userId + ":" + rootOrgId + ":" + networkKeyStr + ":" + networkId + ":" + regionId +
																":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":0" + ":" + System.currentTimeMillis() / 1000;


														if (RFID_LOG == 1) {
															logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() + ",\"timefrompacket\":" + Pkttimestamp + ",\"first\":" + (starttime - System.currentTimeMillis()) + ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"rfidPktNo\":2,\"rfidValue\":" + uuidDet + ",\"status\":" + "autherized" + str + "}";
															log.info(logstring);
														}


														queue.put(new RedisPojo(authKey,authVal));



													}
												}else{
													if (RFID_LOG == 1) {
														logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() + ",\"timefrompacket\":" + Pkttimestamp + ",\"first\":" + (starttime - System.currentTimeMillis()) + ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"rfidPktNo\":2,\"rfidValue\":" + uuidDet + ",\"status\":" + "nfcws cache set" + str + "}";
														log.info(logstring);
													}

												}

											} else {

												String rfidK = "{NFC}WI" + wsId + "SN" + uuidDet;
												String rfidD = regionId + ":" + officeId + ":" + wsId + ":" + uuidDet + ":"
														+ System.currentTimeMillis() / 1000;
												jedis.setex(rfidK, 20, rfidD);
												if(RFID_LOG==1) {
													logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"rfidPktNo\":2,\"rfidValue\":" + uuidDet + ",\"status\":" + "nfc linked" + "}";
													log.info(logstring);
												}

											}
										}
//

//
//							}
										break;

								}
							} else {
								if(RFID_LOG==1) {
									logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "aothercommand expected" + "}";
									log.info(logstring);
								}


							}
						}else{
							if(RFID_LOG==1) {
								logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "sequencenumbermismatch packet:" + sequenceNum + " cache:" + seq + "}";
								log.info(logstring);
							}

						}
					} else {
						if(RFID_LOG==1) {
							logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "dzcache missing" + dzKey + "}";
							log.info(logstring);
						}




					}

				} else {
					if(RFID_LOG==1) {
						logstring = "{\"dID\":" + deviceLongId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + result2.toString() + "\",\"dpkt\":\"" + result3.toString() + "\",\"status\":" + "crc mismatch }";
						log.info(logstring);
					}

				}

			} else {
				if(RFID_LOG==1) {
					logstring = "{\"dID\":" + deviceLongId + ",\"pkt\":\"rfid\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"rpkt\":\"" + "\",\"status\":" + "client cahce not found" + clientKey + " }";
					log.info(logstring);
				}

			}
		}
		catch (Exception e){
			e.printStackTrace();
			log2.fatal("exception in tsrd\n"+e);

		}
	}
}

