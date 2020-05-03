package Helperclasses;

public class Constants {
	
	/* Redis Host */
	public static String HOST = "targusqa.wisilica.com";
	public static Integer PORT = 6379;
	public static String PASSWORD = "redis_root";
	public static Integer CLUSTERENABLE = 0;
	public static final Integer TIMEOUT = 0;
	public static final String CONFIG = "/var/www/html/targus/config.json";
	
	/* Broker */
	public static String BROKER = "ssl://bridgebroker.wisilica.com:8883";
	public static final Integer QOS = 1;
	public static final Integer SEGMENTID = 0;
	
	/*Headers */
	public static final Integer TAG_HEADER = 3;
	public static final Integer MOTHER_TAG_HEADER = 1;
	public static final Integer SENSOR_HEADER = 2;
	public static final Integer OPERATION_HEADER = 4;
	public static final Integer BRIDGE_IP_HEADER = 7;
	public static final Integer BRIDGE_VERSION = 8;
	public static final Integer OTA_COMPLETION_HEADER = 9;
	public static final Integer OTA_HASH_REQUEST_HEADER = 5;
	public static final Integer PACKET_HEADER_OTA_COMPLETE_ACK = 16;
	public static final Integer PACKET_HEADER_OTA_STATUS_DATA = 15;
	
	public static final Integer START_TAG_INDEX = 8;
	public static final Integer START_OPERATION_INDEX = 9;
	
	public static final Integer BRIDGE_OLD = 0;
	public static final Integer BRIDGE_NEW = 1;
	public static final Integer SUCCESS = 1;
	public static final Integer FAILURE = 0;
	public static final Integer REMOTE_OPERATION_FEEDBACK = 1;
	public static final Integer SENSOR = 1;
	public static final int ENABLE_DISABLE_MULTISENSOR_COMMAND_OLD = 21;
	public static final int ENABLE_LISTENER_PWM = 34;
	public static final int DISABLE_LISTENER_PWM = 35;
	public static final int MULTI_SENSOR_ENABLE_NONE_PWM = 3;
	public static final int MULTI_SENSOR_LDR_ENABLE_PWM = 2;
	public static final int MULTI_SENSOR_LDR_MAX_PWM = 4;
	public static final int MULTI_SENSOR_LDR_MIN_PWM = 5;
	public static final int MULTI_SENSOR_PIR_ENABLE_PWM = 1;
	public static final int MULTI_SENSOR_PIR_INTERVAL_PWM = 39;
	public static final int SHUTTER_COMMAND_OLD = 29;
	public static final int SHUTTER_UP_PWM = 3;
	public static final int SHUTTER_DOWN_PWM = 4;
	public static final int SHUTTER_STOP_PWM = 5;
	public static final int SHUTTER_SPIKE_UP_PWM = 1;
	public static final int SHUTTER_SPIKE_DOWN_PWM = 2;
	public static final int SHUTTER_LOCK_PWM = 6;
	public static final int SHUTTER_UNLOCK_PWM = 7;
	public static final int TIME_SYNC_FEEDBACK_COMMAND = 41;
	public static final int OPERATE_STATUS_COMMAND_NEW = 49;
	public static final int STATUS_COMMAND_NEW = 50;
	public static final int GROUP_OPERATE_STATUS_COMMAND_NEW = 52;
	public static final int OTA_UPDATE_STATUS_COMMAND = 11;
	public static final int DEVICE_POWER_CONSUMPTION_STATUS_COMMAND = 43;
	public static final int OPERATE_STATUS_COMMAND_OLD = 3;
	public static final int GROUP_OPERATE_STATUS_COMMAND_OLD = 35;
	public static final Integer OTA_UPDATE_STATUS_SUB_COMMAND = 1;
	public static final Integer TIME_SYNC_FEEDBACK_SUB_COMMAND = 2;
	public static final Integer DEVICE_POWER_CONSUMPTION_STATUS = 43;
	public static final Integer TIME_SYNC_REQUEST_COMMAND = 41;
	public static final Integer TIME_SYNC_REQUEST_SUB_COMMAND = 1;
	public static final Integer TIME_SYNC_FEEDBACK = 5;
	public static final Integer STATUS_BROADCAST_COMMAND = 50;
	public static final Integer STATUS_OPERATION_FEEDBACK = 3;
	public static final Integer DIRECT_OPERATION_FEEDBACK = 2;
	public static final Integer DEVICE_TIME_SYNC_REQUEST = 7;
	public static final Integer SENSOR_OPERATE_DATA_COMMAND = 7;
	
	/* Alert Type */
	public static final Integer NO_ALERT_TYPE = 0;
	public static final Integer BATTERY_ALERT_TYPE = 3;
	public static final Integer TAMPER_ALERT_TYPE = 4;
	public static final Integer TAMPER_AND_BATTERY_ALERT_TYPE = 7;
	
	/* Alert Level */
	public static final Integer TAMPER_ALERT_LEVEL = 1;
	public static final Integer BATTERY_ALERT_LEVEL = 0;
	
	/* Cache Expiry Time */
	public static final Integer ALERT_CACHE_EXPIRY = 3;
	public static final Integer TAG_EXTRA_DATA_CACHE_EXPIRY = 86400;
	public static final Integer OPERATION_DATA_CACHE_EXPIRY = 5;
	public static final Integer HASH_REQUEST_CACHE_EXPIRY = 5;
	public static final Integer OTA_PROGRESS_CACHE_EXPIRY = 5;
	public static final Integer REDIS_TAG_EXIPIRE = 10;
	public static final Integer REDIS_SENSOR_EXIPIRE = 5;
	public static final Integer RSII_EXPIRY = 10;

	/*Packet size length*/
	public static final Integer MIN_OTA_COMPLETE_ACK_PKT_SIZE = 4;
	public static final Integer MAX_OTA_COMPLETE_ACK_PKT_SIZE = 4;


	public static final String TAG_REPORT_PATH = "/var/log/wisilica/";

	/*Targus*/
	public static final Integer STATUS_DOCKPMD_COMMAND = 128;
	public static final Integer STATUS_OPERATION_COMMAND = 129;
	public static final Integer STATUS_ENABLE_DISABLE_COMMAND = 130;
	public static final Integer TELINK_DEVICE_VERSION = 11;

	/*Targus Status*/
	public static final int POWER_DATA = 1;
	public static final int NEWPOWER_DATA = 11;
	public static final int DOCK_STATUS = 8;
	public static final int USB_STATUS = 9;
	public static final int PMD_STATUS = 3;
	public static final int RFID_STATUS = 4;
	public static final int SENSOR_STATUS = 5;
	public static final int MOTION_REPORT = 6;
	public static final int CURRENT_DATA = 10;

	public static void setMqtt(String url){
		BROKER = url;
	}

	public static void setRedis(String redisHost ,Integer redisPort, String redisPassword,Integer clusterEnable){
		HOST = redisHost;
		PORT = redisPort;
		PASSWORD = redisPassword;
		CLUSTERENABLE=clusterEnable;
	}
	
	/*postgres setup variables*/
	public static String POST_DB_HOST = "";
	public static String POST_DB_USER = "";
	public static String POST_DB_PASS = "";
	public static String POST_DB_PORT = "";
	public static String POST_DB_NAME = "";
	
	/*mysql setup variables*/
	public static String MYSQL_DB_HOST = "";
	public static String MYSQL_DB_NAME = "";
	public static String MYSQL_USER_NAME = "";
	public static String MYSQL_PASSWORD = "";
	public static String MYSQL_DB_PORT = "";
	
	/*postgresql setup config function*/
	public static void setPostgres(String dbHost, String dbUsername, String dbPassword, String port, String dbName) {
		POST_DB_HOST = dbHost;
		POST_DB_USER = dbUsername;
		POST_DB_PASS  = dbPassword;
		POST_DB_PORT = port;
		POST_DB_NAME = dbName;
	}
	
	/*mysql setup config function*/
	public static void setMysql(String dbHost, String dbUsername, String dbPassword, String port, String dbName) {
		
		System.out.println(dbHost + ":" + dbUsername + ":" + dbPassword + ":"+ port + ":" + dbName);
		MYSQL_DB_HOST = dbHost;
		MYSQL_USER_NAME = dbUsername;
		MYSQL_PASSWORD  = dbPassword;
		MYSQL_DB_PORT = port;
		MYSQL_DB_NAME = dbName;
	}
	
	/*color*/
	public static String GREEN = "\033[32m";
	public static String BLUE = "\033[34m";
	public static String YELLOW = "\033[33m";
	public static String CYAN = "\033[36m";
	
	/*Enable disable*/
	public static final Integer ENABLE = 1;
	public static final Integer DISABLE = 0;
	
	/*feedback log enable disable*/
	public static Integer FEEDBACK_LOG = 1;
	public static Integer FEEDBACK_PRINT_LOG = 1;
	
	/*rfid log enable disable*/
	public static Integer RFID_LOG = 1;
	public static Integer RFID_PRINT_LOG = 1;
	
	/*sensor log enable disable*/
	public static Integer SENSOR_LOG = 1;
	public static Integer SENSOR_PRINT_LOG = 1;
	
	/*power log enable disable*/
	public static Integer POWER_LOG = 1;
	public static Integer POWER_PRINT_LOG = 1;
	
	/*dock/pmd status log enable disable*/
	public static Integer STATUS_LOG = 1;
	public static Integer STATUS_PRINT_LOG = 1;

	/*dock/pmd status log enable disable*/
	public static Integer VOLTAGE_LOG = 1;
	public static Integer VOLTAGE_PRINT_LOG = 1;
	public static Integer USB_STATUS_LOG = 1;
	public static Integer USB_STATUS_PRINT_LOG = 1;

	public static Integer BRIDGE_COMMAND_LOG = 1;
	public static Integer BRIDGE_COMMAND_PRINT_LOG = 1;

	public static Integer DEVICE_VERSION_LOG = 1;
	public static Integer DEVICE_VERSION_PRINT_LOG = 1;


}
