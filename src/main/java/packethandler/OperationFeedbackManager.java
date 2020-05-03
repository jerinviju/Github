/**
 * 
 */
package packethandler;


import Connectionclasses.RedisDbFactoryClient;
import HelperThreads.Bridgeverifier;
import HelperThreads.Deviceverifier;
import HelperThreads.MysqlThreads;
import HelperThreads.RedisThreads;
import Helperclasses.*;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.FEEDBACK_LOG;


/**
 * @author arshad
 *
 */
public class OperationFeedbackManager implements Runnable {

	static Logger log = Logger.getLogger("feedbackLogger");

	static Logger log2 = Logger.getLogger("exceptionLogger");

	static HashMap<String,String> bridgeMap;
	static HashMap<String,String> deviceMap;

	BlockingQueue<RedisPojo> queue;

	byte[] packet=null;
	long starttime;



	public OperationFeedbackManager(byte[] packet,long starttime) {
		this.packet=packet;
		this.starttime=starttime;
		queue= RedisThreads.getinstance().RedisdataQueue;



	}

	public void run() {
		try {
			String str="";

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

			String logstring = "";

			bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
			deviceMap= Deviceverifier.getinstance().FullDeviceMap;

			Jedis jedis= RedisDbFactoryClient.getInstance().getConnection();



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
				packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX + 1, packet.length - 2);
				byte[] decryptedPacket = null;
				try {
					decryptedPacket = Utility.getInstance().decrypt(packetData, networkKey);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log2.fatal(e);
					log2.fatal("decrypt exception in tsof");

				}

				byte[] crc = CrcGenerator.calculateCrc(Arrays.copyOfRange(decryptedPacket, 2, decryptedPacket.length));


				//write log
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
						String masterKey = "{DMC}DI" + deviceLongId;
						if (commandByte == Constants.STATUS_OPERATION_COMMAND) {
							int operationType = decryptedPacket[15];
							int statusId = decryptedPacket[9];



							if (operationType == 1) {

								int powerport4 = 0, powerport3 = 0, powerport2 = 0, powerport1 = 0;
								int dockStatus = ((decryptedPacket[13] >> 4) & 1);
								int  pmdUpdater = 0;
								if (statusId == 0) {

									int isdock = ((decryptedPacket[12] >> 2) & 1);
									int isCoppernicus = ((decryptedPacket[12] >> 1) & 1);
									int ispmd = ((decryptedPacket[12] & 0xff) & 1);

									int dockReset = ((decryptedPacket[13] >> 5) & 1);




									if(isdock==1 ||isCoppernicus==1){
										pmdUpdater=0;
									}else if(ispmd==1){
										pmdUpdater=1;
									}





									powerport4 = ((decryptedPacket[13] >> 3) & 1);
									powerport3 = ((decryptedPacket[13] >> 2) & 1);
									powerport2 = ((decryptedPacket[13] >> 1) & 1);
									powerport1 = ((decryptedPacket[13] & 0xff) & 1);





									int isMaster = 0;

									if (dockReset!=1) {




										if (jedis.exists(masterKey)) {
											isMaster = 1;

										}






										if (jedis.exists("{NFCSC}ROI" + rootOrgId)) {
											if (Integer.parseInt(jedis.get("{NFCSC}ROI" + rootOrgId).split(":")[2]) == 1) {


												str=deviceLongId+","+ pmdUpdater+","+ powerport1+"," +powerport2+","+ powerport3+","+ powerport4+","+ dockStatus+","+  isMaster+","+ Integer.parseInt(wsId);

												String[] spUpdate ={deviceLongId+"", pmdUpdater+"", powerport1+"", powerport2+"", powerport3+"", powerport4+"", dockStatus+"",  isMaster+"", wsId};
												MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("feedbackupdate",spUpdate));

											}
										} else {

											str=deviceLongId+","+ pmdUpdater+","+ powerport1+"," +powerport2+","+ powerport3+","+ powerport4+","+ dockStatus+","+  isMaster+","+ Integer.parseInt(wsId);

											String[] spUpdate = {deviceLongId+"", pmdUpdater+"", powerport1+"", powerport2+"", powerport3+"", powerport4+"", dockStatus+"",  isMaster+"", wsId};
											MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("feedbackupdate",spUpdate));
										}

									}

									if(FEEDBACK_LOG==1) {
										logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"feedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"p1\":" + powerport1 + ",\"p2\":" + powerport2 + ",\"p3\":" + powerport3 + ",\"p4\":" + powerport4 + ",\"status\":" + dockStatus + ",\"ismaster\":" + isMaster + ",\"status\":" + "sucess" + str+"}";
										log.info(logstring);
									}
//
								} else {
									if(FEEDBACK_LOG==1) {
										logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"feedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"p1\":" + powerport1 + ",\"p2\":" + powerport2 + ",\"p3\":" + powerport3 + ",\"p4\":" + powerport4 + ",\"status\":" + dockStatus + ",\"status\":" + "feedback failed" + "}";
										log.info(logstring);
									}
								}
							}

							else {


								if(FEEDBACK_LOG==1) {
									logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"feedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "some other feedback" + "}";
									log.info(logstring);
								}



							}

						} else if (commandByte == Constants.STATUS_ENABLE_DISABLE_COMMAND) {

							int bridgeEnDis = ((decryptedPacket[11] & 0xff) & 1);


							if(FEEDBACK_LOG==1) {
								logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"masterfeedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"ismaster\":" + bridgeEnDis + "}";
								log.info(logstring);
							}
							if (jedis.exists("{NFCSC}ROI" + rootOrgId)) {
								if (Integer.parseInt(jedis.get("{NFCSC}ROI" + rootOrgId).split(":")[2]) == 1) {

									if(bridgeEnDis==1){
										jedis.set("{DMC}DI"+deviceLongId,"1");
									}else{
										jedis.del("{DMC}DI"+deviceLongId);
									}
									jedis.setex("{MASTERSET}D" + deviceLongId,40,"1");
									String[] spUpdate = {deviceLongId+"", bridgeEnDis+"", sequenceNum+"", deviceShortId+""};
									MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("feedbackupdate",spUpdate));


								}
							}else{

								if(bridgeEnDis==1){
									jedis.set("{DMC}DI"+deviceLongId,"1");
								}else{
									jedis.del("{DMC}DI"+deviceLongId);
								}
								jedis.setex("{MASTERSET}D" + deviceLongId,40,"1");

								String[] spUpdate = {deviceLongId+"", bridgeEnDis+"", sequenceNum+"", deviceShortId+""};
								MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("feedbackupdate",spUpdate));
							}
						} else {
							if(FEEDBACK_LOG==1) {
								logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"masterfeedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"type\":" + deviceType + ",\"rId\":" + regionId + ",\"oId\":" + officeId + ",\"flid\":" + floorId + ",\"wrkId\":" + wsId + ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "some other command" + "}";
								log.info(logstring);
							}
						}
					} else {
						if(FEEDBACK_LOG==1) {
							logstring = "{\"dID\":" + deviceLongId + ",\"dShID\":" + deviceShortId + ",\"pkttype\":\"masterfeedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"seqNo\":" + sequenceNum + ",\"status\":" + "dzkey missing " + dzKey + "}";
							log.info(logstring);
						}
					}

				} else {
					if(FEEDBACK_LOG==1) {
						logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"masterfeedback\",\"epoch\":" + System.currentTimeMillis() +",\"timefrompacket\":" + Pkttimestamp + ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\",\"dpkt\":\"" + result2.toString() + "\",\"status\":" + "crc mismatch" + "}";
						log.info(logstring);
					}



				}

			} else {
				if(FEEDBACK_LOG==1) {
					logstring = "{\"dID\":" + deviceLongId + ",\"pkttype\":\"masterfeedback\",\"epoch\":" + System.currentTimeMillis()  +",\"timefrompacket\":" + Pkttimestamp+ ",\"first\":"+(starttime-System.currentTimeMillis())+ ",\"zId\":" + zoneId + ",\"bId\":" + bridgeId + ",\"pkt\":\"" + result4.toString() + "\"" + "\",\"status\":" + "clientkey missing" + clientKey + "}";
					log.info(logstring);
				}
			}
		}catch (Exception e){
			e.printStackTrace();
			log2.fatal("exception in tsof\n"+e);


		}
	}
}


