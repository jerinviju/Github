package Helperclasses;

import Connectionclasses.MysqlDbFactoryClient;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static Helperclasses.Constants.MYSQL_DB_HOST;


public class CacheCheck {

    private static final Object mutex = new Object();
    static Logger log1 = Logger.getLogger("exceptionLogger");
    Connection cmysql = null;
    private  volatile static HashMap<Integer,Long> LastUpdated;

    private static volatile CacheCheck check;

    private CacheCheck(){
        LastUpdated=new HashMap<>();
    }
    public static CacheCheck getInstance(){
        if(check== null){
            synchronized (mutex) {
                check = new CacheCheck();
            }
        }
        return  check;
    }


    public synchronized void insertData(String[] data) {

        int Key=Integer.parseInt(data[0]);
        if(Key==3|| Key==2){
            Key=2;
        }
        if(LastUpdated.containsKey(Key)){
            if((System.currentTimeMillis()/1000)-LastUpdated.get(Key)<30){
                return;
            }else{
                LastUpdated.put(Key,System.currentTimeMillis()/1000);
            }
        }else{
            LastUpdated.put(Key,System.currentTimeMillis()/1000);
        }

        cmysql = MysqlDbFactoryClient.getInstance().getConnection();

        ResultSet resultSet = null;
        CallableStatement callableStatement= null;


        int sample=0;
        log1.error("cache not present for-- "+data[0]+"--"+data[1]+"--"+data[2]+"--"+data[3]);
        try {
            callableStatement = cmysql.prepareCall("{call dbcheck(?,?,?,?)}");
            callableStatement.setInt(1, Integer.parseInt(data[0]));
            callableStatement.setInt(2, Integer.parseInt(data[1]));
            callableStatement.setInt(3, Integer.parseInt(data[2]));
            callableStatement.setInt(4, Integer.parseInt(data[3]));
            resultSet = callableStatement.executeQuery();

            while (resultSet.next()) {


                sample=Integer.parseInt(resultSet.getString("cacheReload"));

                if(sample==2){
                    log1.error("present in db for device reload");
                    cachereload("pmd-port-mapping");
                    cachereload("sensor-device-zone");
                    cachereload("copernicus-port-mapping");
                    cachereload("sbc-port-mapping");



                }else if(sample==1){

                    cachereload("sbc-port-mapping");
                }
            }

            resultSet.close();
            callableStatement.close();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (callableStatement != null) {
                    callableStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                log1.fatal(e);
                log1.fatal("exception in mysql connection");
            }
        }




    }


    public void cachereload(String str){
        try {
            String url = "http://"+MYSQL_DB_HOST+"/targus/api/public/cache-data/"+str;

            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("Accept", "application/json");

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            //print result
            System.out.println(response.toString());

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
