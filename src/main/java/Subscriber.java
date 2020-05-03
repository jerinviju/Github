import Connectionclasses.MqttClient;
import HelperThreads.Logchecker;
import Helperclasses.Constants;
import Helperclasses.JsonParse;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Subscriber {


    static Logger log2 = Logger.getLogger("exceptionLogger");
    public static void main(String[] args) {





        //Configurations Reading
        JSONObject redisJsonObject;
        JSONObject mqttJsonObject;
        JSONObject clusterEnableObject;
        JSONObject postgresJsonObject;
        JSONObject mysqlJsonObject;
        String fileName="/var/www/html/targus/config.json";
        String content = null;
        try {
            content = new String(Files.readAllBytes(Paths.get(fileName)));
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(content);
            redisJsonObject= (JSONObject) jsonObject.get("redis");
            clusterEnableObject= (JSONObject) jsonObject.get("clusterStatus");
            Constants.setRedis(redisJsonObject.get("host").toString(),Integer.parseInt(redisJsonObject.get("port").toString()),redisJsonObject.get("password").toString(),Integer.parseInt(clusterEnableObject.get("flag").toString()));
            mqttJsonObject= (JSONObject) jsonObject.get("mqtt");
            Constants.setMqtt(mqttJsonObject.get("host").toString());
            postgresJsonObject = (JSONObject) jsonObject.get("postgres");
            Constants.setPostgres(postgresJsonObject.get("host").toString(), postgresJsonObject.get("username").toString(), postgresJsonObject.get("password").toString(), postgresJsonObject.get("port").toString(), postgresJsonObject.get("dbname").toString());
            mysqlJsonObject = (JSONObject) jsonObject.get("php_mysql");
            Constants.setMysql(mysqlJsonObject.get("host").toString(), mysqlJsonObject.get("username").toString(), mysqlJsonObject.get("password").toString(), "3306", mysqlJsonObject.get("dbname").toString());
        }catch (Exception e) {
            log2.fatal(e);
            log2.fatal("connection file read failed");

        }


        //HelperThreads
        new Logchecker().start();



        try {

            JsonParse jsonParse = new JsonParse();
            int fileReadSuccess = jsonParse.parseDetails(Constants.CONFIG);


            if(fileReadSuccess == Constants.SUCCESS) {



                JSONArray clientDetails = jsonParse.getClientDetails();
                for (Object o : clientDetails)
                {

                    JSONObject client = (JSONObject) o;
                    String clientId 	= (String) client.get("clientId");
                    String userName 	= (String) client.get("username");
                    String password 	= (String) client.get("password");
                    String topic 	= (String) client.get("topic");
                    String segmentId = (String) client.get("segment");
                    String name = (String) client.get("name");
                    log2.info("read the details from the config file : "+clientId+":"+userName+":"+":"+password+":"+topic+":"+name+":"+segmentId);

                    new MqttClient("Thread "+name, clientId, userName, password, segmentId, topic).start();
                }

            }else {

                log2.fatal("config file read failure exiting");
            }

        } catch (Exception e) {
            log2.fatal(e);
            log2.fatal("can not start starter function");



        }


    }
}

