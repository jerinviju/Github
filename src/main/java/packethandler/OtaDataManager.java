/**
 * 
 */
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

/**
 * @author arshad
 *
 */
public class OtaDataManager implements Runnable{
	static Logger log= Logger.getLogger("otaLogger");
	static Logger log2 = Logger.getLogger("exceptionLogger");

	static HashMap<String,String> bridgeMap;
	static HashMap<String,String> deviceMap;

	BlockingQueue<RedisPojo> queue;

	byte[] packet=null;
	long starttime;



	public OtaDataManager(byte[] packet,long starttime) {
		// TODO Auto-generated constructor stub
		this.packet=packet;
		this.starttime=starttime;
		queue= RedisThreads.getinstance().RedisdataQueue;



	}



	public void run() {
		try {

			StringBuffer result11 = new StringBuffer();
			for (byte b : packet) {
				result11.append(String.format("%02X ", b));
				result11.append(" "); // delimiter
			}

			System.out.println(result11.toString());


			int rootOrgId = ((packet[3] & 0xff) << 8 | (packet[4] & 0xff));
			int zoneId = ((packet[5] & 0xff) << 8 | (packet[6] & 0xff));
			int bridgeId = ((packet[7] & 0xff) << 8 | (packet[8] & 0xff));
			int userId = 0;

			bridgeMap= Bridgeverifier.getinstance().FullBridgeMap;
			deviceMap= Deviceverifier.getinstance().FullDeviceMap;


			String redisKey = "";
			String redisData = "";
			long timeStamp = System.currentTimeMillis() / 1000;

			String clientCache = "{CLIENT_CACHE}CI" + bridgeId;

			int bridgeLongId = 0;
			String networkKeyStr = "";
			String networkId = "";


			if (bridgeMap.containsKey(clientCache)) {

				String[] userd = bridgeMap.get(clientCache).split(":");
				bridgeLongId = Integer.parseInt(userd[0]);
				networkKeyStr = userd[2];
				networkId = userd[3];

			}

			if (bridgeLongId != 0) {

				int packetLength = ((packet[1] & 0xff) << 8 | (packet[2] & 0xff));
				byte[] packetData = {};

				if (packet[0] == Constants.OTA_HASH_REQUEST_HEADER) {
					if(packet.length==29){
						packet= Arrays.copyOfRange(packet,0,25);
					}

					if (bridgeMap.containsKey(clientCache)) {
						byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);
						packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX, packet.length);


						StringBuffer result12 = new StringBuffer();
						for (byte b : packetData) {
							result12.append(String.format("%02X ", b));
							result12.append(" "); // delimiter
						}

						System.out.println(result12.toString());

						byte[] decryptedPacket = Utility.getInstance().decrypt(packetData, networkKey);


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


						int deviceShortId = ((decryptedPacket[1] & 0xff) & 0xf0) << 4 | (decryptedPacket[0] & 0xff);
						int commandByte = (decryptedPacket[1] & 0xff) & 0x0f;
						int magicNumber = decryptedPacket[2] & 0xff;

						String swVersion = (decryptedPacket[5] & 0xff) + "." + (decryptedPacket[6] & 0xff) + "."
								+ (decryptedPacket[7] & 0xff);
						int fw1 = ((decryptedPacket[8] & 0xff) & 0xf0) >> 4;
						int fw2 = (((decryptedPacket[8] & 0xff) & 0x0f) << 4)
								| (((decryptedPacket[9] & 0xff) & 0xf0) >> 4);
						int fw3 = ((decryptedPacket[9] & 0xff) & 0x0f);
						String fwVersion = fw1 + "." + fw2 + "." + fw3;
						String hwVersion = (decryptedPacket[10] & 0xff) + "." + (decryptedPacket[11] & 0xff) + "."
								+ (decryptedPacket[12] & 0xff);


						StringBuffer vidbuffer = new StringBuffer();
						vidbuffer.append(String.format("%1X", packetData[14]));
						vidbuffer.append(String.format("%02X", packetData[15]));
						String deviceType = vidbuffer.toString().toLowerCase();

						String base64Encoded = Utility.getInstance().byteToBase64(decryptedPacket);


						String deviceKey = "{DZC}ZI" + zoneId + "D" + deviceShortId;
						if (deviceMap.containsKey(deviceKey)) {
							String[] dvc = deviceMap.get(deviceKey).split(":");

							String regionId = dvc[8];
							String officeId = dvc[9];
							String floorId = dvc[10];
							String wsId = dvc[11];


							redisKey = "{LRC}" + "DI" + dvc[0];
							redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + bridgeLongId + ":" + timeStamp + ":1";
							queue.put(new RedisPojo(redisKey,redisData));

							int DeviceLongId = Integer.parseInt(dvc[0]);

							redisKey = "{OTA_HASH" + Constants.SEGMENTID + "}" + "T" + timeStamp + "DL"
									+ String.format("%05d", DeviceLongId) + "MN"
									+ String.format("%05d", magicNumber);

							redisData = deviceShortId + ":" + magicNumber + ":" + hwVersion + ":" + deviceType + ":"
									+ bridgeId + ":" + zoneId + ":" + rootOrgId + ":" + networkKeyStr + ":"
									+ networkId;



							queue.put(new RedisPojo(redisKey,redisData,Constants.HASH_REQUEST_CACHE_EXPIRY));

						} else {
							System.out.println("Hash Request Device Redis key not found");

						}

					} else {
						System.out.println("Client key not found");

					}

				} else if ((packet[0] == Constants.OTA_COMPLETION_HEADER)) {
					if (bridgeMap.containsKey(clientCache)) {
						byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);
						packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX, packet.length);
						//log.info("hash request processing bytes");
						System.out.println("ota completion -processing bytes");
						//System.out.println(Arrays.toString(packetData));

						byte[] decryptedPacket = Utility.getInstance().decrypt(packetData, networkKey);

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


						byte[] crc = CrcGenerator.calculateCrc(
								Arrays.copyOfRange(decryptedPacket, 2, decryptedPacket.length - 2));
						if (((crc[0] & 0xff) == (decryptedPacket[15] & 0xff)) && ((crc[1] & 0xff) == (decryptedPacket[16] & 0xff))) {

							int deviceShortId = ((decryptedPacket[1] & 0xff) & 0xf0) << 4 | (decryptedPacket[0] & 0xff);
							int commandByte = (decryptedPacket[1] & 0xff) & 0x0f;
							int magicNumber = decryptedPacket[2] & 0xff;

							String swVersion = (decryptedPacket[5] & 0xff) + "." + (decryptedPacket[6] & 0xff) + "."
									+ (decryptedPacket[7] & 0xff);
							int fw1 = ((decryptedPacket[8] & 0xff) & 0xf0) >> 4;
							int fw2 = (((decryptedPacket[8] & 0xff) & 0x0f) << 4)
									| (((decryptedPacket[9] & 0xff) & 0xf0) >> 4);
							int fw3 = ((decryptedPacket[9] & 0xff) & 0x0f);
							String fwVersion = fw1 + "." + fw2 + "." + fw3;
							String hwVersion = (decryptedPacket[10] & 0xff) + "." + (decryptedPacket[11] & 0xff) + "."
									+ (decryptedPacket[12] & 0xff);
							int deviceTypeMajor = ((decryptedPacket[13] & 0xff) << 8 | (decryptedPacket[14] & 0xff));
							int deviceTypeMinor = decryptedPacket[15] & 0xff;
							String deviceType = Integer.toHexString(deviceTypeMajor)
									+ Integer.toHexString(deviceTypeMinor);
							int otaDeviceType = Integer.parseInt(deviceTypeGet(deviceType));

							String base64Encoded = Utility.getInstance().byteToBase64(decryptedPacket);


							String deviceKey = "{DZC}ZI" + zoneId + "D" + deviceShortId;
							if (deviceMap.containsKey(deviceKey)) {
								String[] dvc = deviceMap.get(deviceKey).split(":");

								String regionId = dvc[8];
								String officeId = dvc[9];
								String floorId = dvc[10];
								String wsId = dvc[11];
								//String deviceType = dvc[4];


								redisKey = "{LRC}" + "DI" + dvc[0];
								redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + bridgeLongId + ":" + timeStamp;
								queue.put(new RedisPojo(redisKey,redisData));

								int DeviceLongId = Integer.parseInt(dvc[0]);

								redisKey = "{OTA_REQUEST_REINITIATE}" + "T" + timeStamp + "SO"
										+ String.format("%05d", zoneId) + "DT"
										+ String.format("%05d", otaDeviceType) + "HT"
										+ String.format("%05d", hwVersion);
								redisData = rootOrgId + ":" + zoneId + ":" + deviceType + ":" + magicNumber + ":" + hwVersion + ":" + swVersion + ":" + userId;
								log.info(redisKey + "---" + redisData);
								System.out.println(redisKey + "---" + redisData);

								queue.put(new RedisPojo(redisKey,redisData,Constants.HASH_REQUEST_CACHE_EXPIRY));


							} else {
								System.out.println("Hash Request Device Redis key not found");

							}

						}

					} else {
						System.out.println("Client key not found");

					}

				} else if ((packet[0] == Constants.PACKET_HEADER_OTA_COMPLETE_ACK)) {
					if (bridgeMap.containsKey(clientCache)) {
						byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);
						packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX, packet.length);

						System.out.println("ota completion -processing bytes");


						byte[] decryptedPacket = Utility.getInstance().decrypt(packetData, networkKey);

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


						byte[] crc = CrcGenerator.calculateCrc(
								Arrays.copyOfRange(decryptedPacket, 2, decryptedPacket.length - 2));
						if (((crc[0] & 0xff) == (decryptedPacket[15] & 0xff)) && ((crc[1] & 0xff) == (decryptedPacket[16] & 0xff))) {

							int deviceShortId = ((decryptedPacket[1] & 0xff) & 0xf0) << 4 | (decryptedPacket[0] & 0xff);
							int commandByte = (decryptedPacket[1] & 0xff) & 0x0f;
							int magicNumber = decryptedPacket[2] & 0xff;

							String swVersion = (decryptedPacket[5] & 0xff) + "." + (decryptedPacket[6] & 0xff) + "."
									+ (decryptedPacket[7] & 0xff);
							int fw1 = ((decryptedPacket[8] & 0xff) & 0xf0) >> 4;
							int fw2 = (((decryptedPacket[8] & 0xff) & 0x0f) << 4)
									| (((decryptedPacket[9] & 0xff) & 0xf0) >> 4);
							int fw3 = ((decryptedPacket[9] & 0xff) & 0x0f);
							String fwVersion = fw1 + "." + fw2 + "." + fw3;
							String hwVersion = (decryptedPacket[10] & 0xff) + "." + (decryptedPacket[11] & 0xff) + "."
									+ (decryptedPacket[12] & 0xff);
							int deviceTypeMajor = ((decryptedPacket[13] & 0xff) << 8 | (decryptedPacket[14] & 0xff));
							int deviceTypeMinor = decryptedPacket[15] & 0xff;
							String deviceType = Integer.toHexString(deviceTypeMajor)
									+ Integer.toHexString(deviceTypeMinor);
							int otaDeviceType = Integer.parseInt(deviceTypeGet(deviceType));
							String base64Encoded = Utility.getInstance().byteToBase64(decryptedPacket);


							String deviceKey = "{DZC}ZI" + zoneId + "D" + deviceShortId;
							if (deviceMap.containsKey(deviceKey)) {
								String[] dvc = deviceMap.get(deviceKey).split(":");

								String regionId = dvc[8];
								String officeId = dvc[9];
								String floorId = dvc[10];
								String wsId = dvc[11];
								//String deviceType = dvc[4];


								redisKey = "{LRC}" + "DI" + dvc[0];
								redisData = rootOrgId + ":" + regionId + ":" + officeId + ":" + floorId + ":" + zoneId + ":" + wsId + ":" + bridgeLongId + ":" + timeStamp;
								queue.put(new RedisPojo(redisKey,redisData));

								int DeviceLongId = Integer.parseInt(dvc[0]);

								redisKey = "{OTA_REQUEST_REINITIATE}" + "T" + timeStamp + "SO"
										+ String.format("%05d", zoneId) + "DT"
										+ String.format("%05d", otaDeviceType) + "HT"
										+ String.format("%05d", hwVersion);
								redisData = rootOrgId + ":" + zoneId + ":" + otaDeviceType + ":" + magicNumber + ":" + hwVersion + ":" + swVersion + ":" + userId;
								log.info(redisKey + "---" + redisData);
								System.out.println(redisKey + "---" + redisData);

								queue.put(new RedisPojo(redisKey,redisData,Constants.HASH_REQUEST_CACHE_EXPIRY));

							} else {
								System.out.println("Hash Request Device Redis key not found");

							}

						}

					} else {
						System.out.println("Client key not found");

					}

				} else if ((packet[0] == Constants.PACKET_HEADER_OTA_STATUS_DATA)) {

					if(packet.length==22){
						packet= Arrays.copyOfRange(packet,0,18);
					}

					byte[] networkKey = Utility.getInstance().base64ToByte(networkKeyStr);
					packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX, packet.length);
					//log.info("hash request processing bytes");
					System.out.println("ota status -processing bytes");
					System.out.println(Arrays.toString(packetData));

					StringBuffer result1 = new StringBuffer();
					for (byte b : packet) {
						result1.append(String.format("%02X ", b));
						result1.append(" "); // delimiter
					}

					StringBuffer result2 = new StringBuffer();
					for (byte b : packetData) {
						result2.append(String.format("%02X ", b));
						result2.append(" "); // delimiter
					}


					StringBuffer vidbuffer = new StringBuffer();
					vidbuffer.append(String.format("%1X", packetData[0]));
					vidbuffer.append(String.format("%02X", packetData[1]));
					String deviceType = vidbuffer.toString().toLowerCase();
					System.out.print("devcetype " + deviceType);
					log.info("devcetype " + deviceType);
					int otaDeviceType = Integer.parseInt(deviceType);
					int magicNumber = packetData[2] & 0xff;//
					int status = packetData[3] & 0xff;
					int pass = packetData[4] & 0xff;
					int remainingBlk = ((packetData[5] & 0xff) << 8) | (packetData[6] & 0xff);
					int totalBlk = ((packetData[7] & 0xff) << 8) | (packetData[8] & 0xff);

					redisKey = "{OTA_PROGRESS_CACHE}" + "RO" + rootOrgId + "SO"
							+ String.format("%05d", zoneId) + "DT"
							+ String.format("%05d", otaDeviceType) + "BI"
							+ String.format("%05d", bridgeId);

					System.out.println("redis key : " + redisKey);
					log.info("redis key : " + redisKey);
					redisData = rootOrgId + ":" + zoneId + ":" + deviceType + ":" + magicNumber + ":" + status + ":" + bridgeId + ":" + pass + ":" + remainingBlk + ":" + totalBlk;

					log.info("redis value : " + redisData);
					System.out.println(redisKey + "---" + redisData);
					queue.put(new RedisPojo(redisKey,redisData,Constants.HASH_REQUEST_CACHE_EXPIRY));


				} else {
					System.out.println("Header not valid");

				}

			} else {
				System.out.println(clientCache + " ota Client key not found");

			}
		}catch (Exception e){
			e.printStackTrace();
			log2.fatal("exception in ota\n"+e);
		}
		return;
	}

	public String deviceTypeGet( String deviceTypehex) {
		String deviceType = "";
		switch (deviceTypehex) {

			case "d02":
				deviceType = "13002";

				break;
			case "d03":
				deviceType = "13003";

				break;
			case "213":
				deviceType = "2019";

				break;
			case "306":
				deviceType = "3006";

				break;

		}
		return deviceType;
	}
}


