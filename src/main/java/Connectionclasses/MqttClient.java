package Connectionclasses;


import Helperclasses.Constants;
import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import packethandler.*;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MqttClient extends Thread implements MqttCallbackExtended {

	static Logger log2 = Logger.getLogger("exceptionLogger");


	MemoryPersistence persistence = new MemoryPersistence();
	String mThreadName = null;
	String mClientId = null;
	String mUserName = null;
	String mUserPassword = null;
	String mSegmentId = null;
	String mTopic = null;



	MqttAsyncClient Client = null;
	static int tagcounter = 1;


	ExecutorService executor =null;
	Connection cpost = null;

	private DockPmdStatusManager dockPmdStatusManager = null;
	private SensorTriggerManager sensorTriggerManager = null;
	private RFIDManager rfidManager = null;
	private OperationFeedbackManager operationFeedbackManager = null;
	private CurrentVoltageDataManager currentVoltageDataManager =null;
	private OtaDataManager otaDataManager = null;

	private NewPowerDataManager newPowerDataManager = null;
	private UsbStatusdataManager usbStatusdataManager = null;
	private DeviceVersionManager deviceVersionManager =null;
	private CommandDataManager commandDataManager = null;




	public MqttClient(String message, String clientId, String username, String password, String segmentid,
                      String topic) {

		super();
		this.mThreadName = message;
		this.mClientId = clientId;
		this.mUserName = username;
		this.mUserPassword = password;
		this.mSegmentId = segmentid;
		this.mTopic = topic;


		executor=Executors.newCachedThreadPool();






	}

	@Override
	public void run() {
		super.run();
		register();
	}

	public void register() {

		try {
			if (Client == null) {
				Client = new MqttAsyncClient(Constants.BROKER, mClientId, persistence);
				MqttConnectOptions connOpts = new MqttConnectOptions();
				connOpts.setUserName(mUserName);
				connOpts.setPassword(mUserPassword.toCharArray());
				connOpts.setConnectionTimeout(0);
				connOpts.setKeepAliveInterval((60 * 3)-1);
				connOpts.setAutomaticReconnect(true);


//
				connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
				connOpts.setCleanSession(false);
				Client.setCallback(this);

				Client.connect(connOpts);

				log2.info(mThreadName + "trying to Connect " +Constants.BROKER+"client "+mClientId);

			}


		} catch (Exception me) {
            log2.fatal(me);
			if (me instanceof MqttException) {
                log2.fatal("mqtt connection exception reason " + ((MqttException) me).getReasonCode());
            }
		}

	}

	@Override
	public void connectionLost(Throwable arg0) {

		log2.error("connection lost client"+this.mClientId);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
        log2.info("delivery complete for "+this.mClientId);

	}

	@Override
	public void connectComplete(boolean reconnect, String message) {
		// TODO Auto-generated method stub
		if (reconnect) {
            log2.error("reconnecting client "+this.mClientId);
			try {
                Client.subscribe(mTopic, Constants.QOS);
                log2.info("client "+this.mClientId+" subscribed to topic "+this.mTopic);
            }catch(Exception e){

				log2.fatal(e);
				log2.fatal("client reconnecting failed "+mClientId);
            }
		}else{
		    try {
                Client.subscribe(mTopic, Constants.QOS);
                log2.info("client "+this.mClientId+" subscribed to topic "+this.mTopic);
            }catch (Exception e){
				log2.fatal(e);
				log2.fatal("client connecting failed "+mClientId);

            }
        }
	}


	@Override
	public void messageArrived(String topic, MqttMessage message) {

		Long starttime=System.currentTimeMillis();
		try {

			String[] topicDetails = topic.split("/");
			byte[] packet =message.getPayload();

			switch (topicDetails[1]) {
				case "tsts":
					if (packet.length == 28 ||packet.length == 32) {
						dockPmdStatusManager = new DockPmdStatusManager( packet,starttime);
					}
					break;
				case "tsst":
					if (packet.length == 28 ||packet.length == 32) {
						sensorTriggerManager = new SensorTriggerManager( packet,starttime);
					}

					break;
				case "tsrd":
					if (packet.length == 28 ||packet.length == 32) {
						rfidManager = new RFIDManager(packet,starttime );
					}

					break;
				case "tsof":
					if (packet.length == 28 ||packet.length == 32) {
						operationFeedbackManager = new OperationFeedbackManager(packet,starttime);
					}

					break;
				case "tsor":
					otaDataManager = new OtaDataManager(packet,starttime);
					break;
				case "tspd":
					if (packet.length == 28 ||packet.length == 32) {
						newPowerDataManager = new NewPowerDataManager( packet,starttime);
					}

					break;
				case "tsvc":
					if (packet.length == 28 ||packet.length == 32) {
						currentVoltageDataManager = new CurrentVoltageDataManager(packet,starttime);
					}

					break;
				case "tsus":
					if (packet.length == 28 ||packet.length == 32) {
						usbStatusdataManager = new UsbStatusdataManager(  packet,starttime);
					}

					break;
				case "tsbc":
					commandDataManager = new CommandDataManager( packet);
					executor.execute(commandDataManager);
					break;
				case "tsop":
					otaDataManager = new OtaDataManager(packet,starttime);
					break;
				case "tsdv":
					if (packet.length == 28 ||packet.length == 32) {
						deviceVersionManager = new DeviceVersionManager(packet,starttime);
					}

					break;

				default:  log2.fatal("wrong topic");



			}
		}catch (Exception e){
			e.printStackTrace();
			log2.fatal(e);
			log2.fatal("error in publishing data from handler to parsing class");
		}






		return;
	}
}