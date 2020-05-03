
package packethandler;


import HelperThreads.Bridgeverifier;
import HelperThreads.MysqlThreads;
import HelperThreads.RedisThreads;
import Helperclasses.Constants;
import Helperclasses.RedisPojo;
import Helperclasses.SpPojo;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static Helperclasses.Constants.BRIDGE_COMMAND_LOG;


/**
 * @author arshad
 *
 */
public class CommandDataManager implements Runnable {


	static Logger log  = Logger.getLogger("bridgeLogger");
	static Logger log2 = Logger.getLogger("exceptionLogger");

	private BlockingQueue<RedisPojo> queue;
 	static HashMap<String,String> bridgeMap;

	byte[] packet =null;







	public CommandDataManager(byte[] packet) {

		try {
			queue=RedisThreads.getinstance().RedisdataQueue;


			this.packet=packet;

		}catch ( Exception e){
			e.printStackTrace();
		}


	}


	@Override
	public void run() {

		try {

			bridgeMap= Bridgeverifier.FullBridgeMap;

			long Pkttimestamp=0;
			String logstring;


			int rootOrgId = ((packet[3] & 0xff) << 8 | (packet[4] & 0xff));
			int zoneId = ((packet[5] & 0xff) << 8 | (packet[6] & 0xff));
			int bridgeId = ((packet[7] & 0xff) << 8 | (packet[8] & 0xff));


			String clientCache = "{CLIENT_CACHE}CI" + bridgeId;
			int bridgeLongId = 0;

			StringBuilder result1 = new StringBuilder();
			for (byte b : packet) {
				result1.append(String.format("%02X ", b));
				result1.append(" "); // delimiter
			}

			if (bridgeMap.containsKey(clientCache)) {
				String[] userd = bridgeMap.get(clientCache).split(":");
				bridgeLongId = Integer.parseInt(userd[0]);
			}
			if (bridgeLongId != 0) {


				int packetLength = ((packet[1] & 0xff) << 8 | (packet[2] & 0xff));
				String IP = "";
				String version = "";

				byte[] packetData;

				if (packetLength == (packet.length - 3)) {
					int command = packet[9] & 0xff;
					if (command == Constants.BRIDGE_IP_HEADER) {

						if(packet.length==27){
							Pkttimestamp=((packet[23] & 0xff) << 24 | (packet[24] & 0xff) << 16| (packet[25] & 0xff) << 8 | (packet[26] & 0xff));
							packet= Arrays.copyOfRange(packet, 0, 23);
						}
						packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX + 1, packet.length);


						try {
							IP = new String(packetData, StandardCharsets.UTF_8);
						} catch (Exception e) {
							e.printStackTrace();



							log2.fatal("exception in IP parsing" + e);

							if (BRIDGE_COMMAND_LOG == 1) {
								logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp +  ",\"pkt\":\"" + result1.toString() + "\",\"dpkt\":\"nil\",\"subCommand\":\"ip\",\"value\":\"" + IP + "\"" + ",\"status\":\"" + "exception in ip_parsing" + "\"}";
								log.info(logstring);
							}
						}

						queue.put(new RedisPojo("{BRIDGEIP}BI" + bridgeLongId,IP));

						if (BRIDGE_COMMAND_LOG == 1) {
							logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result1.toString() + "\",\"dpkt\":\"nil\",\"subCommand\":\"ip\",\"value\":\"" + IP + "\"" + ",\"status\":\"" + "success" + "\"}";
							log.info(logstring);
						}

					} else if (command == Constants.BRIDGE_VERSION) {
						if (packet.length == 18 || packet.length == 22) {
							if(packet.length==22){
								Pkttimestamp=((packet[18] & 0xff) << 24 | (packet[19] & 0xff) << 16| (packet[20] & 0xff) << 8 | (packet[21] & 0xff));
								packet= Arrays.copyOfRange(packet, 0, 18);
							}
							packetData = Arrays.copyOfRange(packet, Constants.START_OPERATION_INDEX + 1, packet.length);


							try {
								version = new String(packetData, StandardCharsets.UTF_8);
								int str1 = Integer.parseInt(version.substring(0, 2));
								int str2 = Integer.parseInt(version.substring(3, 5));
								int str3 = Integer.parseInt(version.substring(6, 8));
								version = str1 + "." + str2 + "." + str3;


								String[] arr = new String[4];
								arr[0] = rootOrgId + "";
								arr[1] = zoneId + "";
								arr[2] = bridgeLongId + "";
								arr[3] = version;


								MysqlThreads.getinstance().MysqlSpdataQueue.put(new SpPojo("versionupdate",arr));


								if (BRIDGE_COMMAND_LOG == 1) {
									logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result1.toString() + "\",\"dpkt\":\"nil\",\"subCommand\":\"version\",\"value\":\"" + version + "\"}";
									log.info(logstring);
								}


							} catch (Exception e) {
								e.printStackTrace();



								log2.fatal("exception in version parsing" + e);
								if (BRIDGE_COMMAND_LOG == 1) {
									logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result1.toString() + "\",\"dpkt\":\"nil\",\"subCommand\":\"version\",\"value\":\"" + version + "\"" + ",\"status\":\"" + "exception in version_parsing" + "\"}";
									log.info(logstring);
								}
							}

						} else {
							if (BRIDGE_COMMAND_LOG == 1) {
								logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result1.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + "packet length wrong reqiured 18 got" + packet.length + "\"}";
								log.info(logstring);
							}
						}
					}
				} else {
					if (BRIDGE_COMMAND_LOG == 1) {
						logstring = "{\"bID\":" + bridgeLongId + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" +",\"timefrompacket\":" + Pkttimestamp+ bridgeId + ",\"pkt\":\"" + result1.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + " total packet length wrong " + "\"}";
						log.info(logstring);
					}
				}
			} else {
				if (BRIDGE_COMMAND_LOG == 1) {
					logstring = "{\"dID\":" + "---" + ",\"dShID\":" + "\"---\"" + ",\"pkttype\":\"bcommnd\",\"epoch\":" + System.currentTimeMillis() / 1000 + ",\"type\":" + "\"---\"" + ",\"rId\":" + "\"---\"" + ",\"oId\":\"---\",\"flid\":\"---\",\"wrkId\":\"---\",\"zId\":" + zoneId + ",\"bId\":" + bridgeId +",\"timefrompacket\":" + Pkttimestamp+ ",\"pkt\":\"" + result1.toString() + "\",\"subCommand\":\"version\",\"status\":\"" + " bridgeid wrong from packet :" + bridgeId + "\"}";
					log.info(logstring);
				}


			}

		}catch (Exception e){
			e.printStackTrace();

			log2.fatal("exception in tsvc\n"+e);



		}

	}
}


