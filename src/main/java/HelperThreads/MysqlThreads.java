package HelperThreads;

import Connectionclasses.MysqlDbFactoryClient;
import Connectionclasses.RedisDbFactoryClient;
import Helperclasses.SpPojo;
import Helperclasses.packetcreate;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MysqlThreads implements Runnable{

    static MysqlThreads mysqlThreads=null;

    public BlockingQueue<String> MysqldataQueue;
    public BlockingQueue<SpPojo> MysqlSpdataQueue;
    ExecutorService executor =null;
    ExecutorService spexecutor =null;
    private static final Object mutex = new Object();

    private MysqlThreads(){
        try {
            executor= Executors.newFixedThreadPool(3);
            MysqldataQueue = new ArrayBlockingQueue<>(500000);

            spexecutor= Executors.newFixedThreadPool(3);
            MysqlSpdataQueue = new ArrayBlockingQueue<>(500000);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static MysqlThreads getinstance(){
        if(mysqlThreads==null){
            synchronized (mutex) {
                mysqlThreads = new MysqlThreads();
            }
        }
        return mysqlThreads;
    }

    @Override
    public void run() {
        while(true){
            try{
                List<String> sqlStrings = new ArrayList<>();
                MysqldataQueue.drainTo(sqlStrings);
                if(sqlStrings.size()>0) {
                    Task task = new Task(MysqlDbFactoryClient.getInstance().getConnection(), sqlStrings);
                    executor.execute(task);
                }

                List<SpPojo> spcalls = new ArrayList<>();
                MysqlSpdataQueue.drainTo(spcalls);
                if(spcalls.size()>0) {
                    SpTask task = new SpTask(MysqlDbFactoryClient.getInstance().getConnection(), spcalls);
                    spexecutor.execute(task);
                }
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    class Task implements  Runnable{

        Connection cmysql;
        Statement stmt;
        List<String> sqlStrings;

        private Task(Connection cmysql,List<String> sqlStrings){
            this.cmysql=cmysql;
            this.sqlStrings=sqlStrings;
        }

        @Override
        public void run() {
            try {
                if(cmysql!=null) {
                    stmt = cmysql.createStatement();
                    for (String sql : sqlStrings
                    ) {
                        stmt.addBatch(sql);
                    }

                    stmt.executeBatch();
                    stmt.close();
                }else{
                    for (String sql : sqlStrings
                    ) {
                        MysqldataQueue.put(sql);
                    }
                }
            }catch (Exception e){

            }

        }

    }

    class SpTask implements  Runnable{

        Connection cmysql;
        Statement stmt;
        List<SpPojo> sqlStrings;

         Logger log1 = Logger.getLogger("exceptionLogger");

        private SpTask(Connection cmysql,List<SpPojo> sqlStrings){
            this.cmysql=cmysql;
            this.sqlStrings=sqlStrings;
        }

        @Override
        public void run() {
            try {
                if(cmysql!=null) {

                    for (SpPojo sps : sqlStrings
                    ) {
                        switch(sps.getSp()){
                            case "versionupdate":
                                VersianUpdate(sps.getArgs());
                                break;
                            case "dockstatus":
                                dockstatusupdate(sps.getArgs());
                                break;
                            case "pmdstatus":
                                pmdstatus(sps.getArgs());
                                break;
                            case "feedbackupdate":
                                Feedbackupdate(sps.getArgs());
                                break;
                        }
                    }


                }else{
                    for (SpPojo sql : sqlStrings
                    ) {
                        MysqlSpdataQueue.put(sql);
                    }
                }
            }catch (Exception e){

            }

        }




        public void VersianUpdate(String[] data) {

             Jedis jedis= RedisDbFactoryClient.getInstance().getConnection();






            log1.info(" version STATUS UPDATE IN MYSQL-- "+data[3]+"--"+data[2]);

            ResultSet resultSet = null;
            CallableStatement callableStatement= null;

            try {

                callableStatement = this.cmysql.prepareCall("{call version_status_update(?,?,?,?)}");
                callableStatement.setInt(1, Integer.parseInt(data[0]));
                callableStatement.setInt(2, Integer.parseInt(data[1]));
                callableStatement.setInt(3, Integer.parseInt(data[2]));
                callableStatement.setString(4, data[3]);

                resultSet = callableStatement.executeQuery();

                while (resultSet.next()) {
                    int root_org_id=Integer.parseInt(data[0]);
                    int zoneid=Integer.parseInt(data[1]);
                    int sample=Integer.parseInt(resultSet.getString("status"));
                    String samplemesage=resultSet.getString("message");
                    log1.info(" SQL result-- "+sample +"---"+samplemesage+"---"+data[3]);

                    if(sample==1){
                        try {
                            int networkid = resultSet.getInt("networkId");
                            int devicetype = resultSet.getInt("deviceType");
                            String hversion = resultSet.getString("hardwareVersion");
                            String publishTopic = "9/tsoc/" + data[0] + "/" + data[1];

                            int packetheader = 12;
                            int packetlength = 15;

                            int magicnumber = 0;

                            int otapacketspersecond = 5;
                            int OTAFirmwareCommand = 2;
                            int $OTAStatusPacketInterval = 5;

                            String networkid_str= stringbuilder(networkid, 4);

                            String devicetype_str= stringbuilder(devicetype, 4);


                            String[] hardwareversion=hversion.split("\\.");


                            String publishstr = stringbuilder(packetheader, 2) + stringbuilder(packetlength, 4) + stringbuilder(root_org_id, 4) + stringbuilder(zoneid, 4)
                                    + networkid_str + stringbuilder(magicnumber, 2) +devicetype_str +
                                    stringbuilder(Integer.parseInt(hardwareversion[0]), 2) + stringbuilder(Integer.parseInt(hardwareversion[1]), 2) + stringbuilder(Integer.parseInt(hardwareversion[2]), 2) +
                                    stringbuilder(otapacketspersecond, 2) + stringbuilder(OTAFirmwareCommand, 2) + stringbuilder($OTAStatusPacketInterval, 2);

                            jedis.publish("ota", root_org_id + ":" + zoneid + ":" + publishstr);

                            log1.info("initiate packet send"+root_org_id + ":" + zoneid + ":" + publishstr);

                        }catch (Exception e ){
                            log1.fatal(e);
                            log1.fatal("exception in version update Manager manager for initaiating");

                        }

                    }
                    else if (sample==0) {

                        try {
                            int devicelongid = resultSet.getInt("deviceID");
                            int devicetype = Integer.parseInt(jedis.get("{DTC}DI" + devicelongid).split(":")[1]);
                            log1.info(" Remote operatiion for slave to master for device longid: " + devicelongid);
                            if (!jedis.exists("{DMC}DI" + devicelongid)) {
                                int seq1 = hello( devicelongid);
                                int networkid = resultSet.getInt("networkId");
                                String networkkey = resultSet.getString("networkkey");
                                int deviceshortid = resultSet.getInt("deviceshortID");
                                try {
                                    String publishstr = root_org_id + ":" + zoneid + ":" + new packetcreate().packetcreation(networkid, deviceshortid, networkkey, root_org_id, zoneid, seq1, devicetype);
                                    jedis.publish("remote-operation", publishstr);
                                    jedis.setex("{MASTERSET}D"+devicelongid,40,System.currentTimeMillis()/1000+"");

                                    log1.info(" DMC cache does not exist " + devicelongid + " packet: " + publishstr);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    log1.info("error in packet:" + e.getMessage() + e.toString());
                                }

                                try {

                                    stmt = this.cmysql.createStatement();
                                    String sql = "UPDATE tbl_devices SET is_master_dock=0 ,timestamp = UNIX_TIMESTAMP(NOW(6)) WHERE  device_id=" + devicelongid;
                                    log1.info(sql);
                                    stmt.addBatch(sql);
                                    stmt.executeBatch();
                                    stmt.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    log1.info("error in mysql : " + e.getMessage() + e.toString());
                                }
                            }
                        }catch(Exception e){
                            log1.fatal(e);
                            log1.fatal("exception in version update Manager manager for master");
                        }
                    }
                    else{
                        System.out.println("bridge version updated");
                    }
                }

                callableStatement.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally {
                try {
                    if(resultSet != null) {
                        resultSet.close();
                    }
                    if(callableStatement != null) {
                        callableStatement.close();
                    }
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

        public int hello(int id){
            int sample=0;
            ResultSet resultSet = null;
            CallableStatement callableStatement= null;
            try {
                callableStatement = cmysql.prepareCall("{call seq_get_update(?,?,?,?)}");
                callableStatement.setInt(1, id);
                callableStatement.setInt(2, 2);
                callableStatement.setInt(3, 0);
                callableStatement.setInt(4, 0);
                resultSet = callableStatement.executeQuery();

                while (resultSet.next()) {


                    sample=Integer.parseInt(resultSet.getString("remote_seq_num"));
                }

                resultSet.close();
                callableStatement.close();


            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    if(resultSet != null) {
                        resultSet.close();
                    }
                    if(callableStatement != null) {
                        callableStatement.close();
                    }
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            return sample;
        }

        public void dockstatusupdate(String[] data){

            if(data.length == 10) {


                ResultSet resultSet = null;
                CallableStatement callableStatement= null;

                try {

                    callableStatement = this.cmysql.prepareCall("{call copernicus_data_update(?,?,?,?,?,?,?,?,?,?)}");
                    callableStatement.setInt(1, Integer.parseInt(data[0]));
                    callableStatement.setInt(2, Integer.parseInt(data[1]));
                    callableStatement.setInt(3, Integer.parseInt(data[2]));
                    callableStatement.setInt(4, Integer.parseInt(data[3]));
                    callableStatement.setInt(5,Integer.parseInt( data[4]));
                    callableStatement.setInt(6, Integer.parseInt(data[5]));
                    callableStatement.setInt(7,Integer.parseInt( data[6]));
                    callableStatement.setInt(8, Integer.parseInt(data[7]));
                    callableStatement.setInt(9, Integer.parseInt(data[8]));
                    callableStatement.setInt(10, Integer.parseInt(data[9]));

                    resultSet = callableStatement.executeQuery();
                    while (resultSet.next()) {

                    }
                    resultSet.close();
                    callableStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {
                    try {
                        if(resultSet != null) {
                            resultSet.close();
                        }
                        if(callableStatement != null) {
                            callableStatement.close();
                        }
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }else {





                ResultSet resultSet = null;
                CallableStatement callableStatement= null;

                try {
                    System.out.println("call dock_data_update "+data[0]+","+data[1]+","+data[2]+","+data[3]+","+data[4]+","+data[5]+","+data[6]+","+data[7]+","+data[8]+","+data[9]+","+data[10]+","+data[11]+","+data[12]+","+data[13]+","+data[14]+","+data[15]+","+data[16]+","+data[17]+","+data[18]+","+data[19]+","+data[20]+","+data[21]+","+data[22]+","+data[23]);
                    callableStatement = this.cmysql.prepareCall("{call dock_data_update(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
                    callableStatement.setInt(1, Integer.parseInt(data[0]));
                    callableStatement.setInt(2, Integer.parseInt(data[1]));
                    callableStatement.setInt(3, Integer.parseInt(data[2]));
                    callableStatement.setInt(4, Integer.parseInt(data[3]));
                    callableStatement.setInt(5, Integer.parseInt(data[4]));
                    callableStatement.setInt(6,Integer.parseInt( data[5]));
                    callableStatement.setInt(7, Integer.parseInt(data[6]));
                    callableStatement.setInt(8, Integer.parseInt(data[7]));
                    callableStatement.setInt(9, Integer.parseInt(data[8]));
                    callableStatement.setInt(10,Integer.parseInt( data[9]));
                    callableStatement.setInt(11, Integer.parseInt(data[10]));
                    callableStatement.setInt(12, Integer.parseInt(data[11]));
                    callableStatement.setInt(13, Integer.parseInt(data[12]));
                    callableStatement.setInt(14,Integer.parseInt( data[13]));
                    callableStatement.setInt(15, Integer.parseInt(data[14]));
                    callableStatement.setInt(16, Integer.parseInt(data[15]));
                    callableStatement.setInt(17,Integer.parseInt( data[16]));
                    callableStatement.setInt(18, Integer.parseInt(data[17]));
                    callableStatement.setInt(19, Integer.parseInt(data[18]));
                    callableStatement.setInt(20,Integer.parseInt( data[19]));
                    callableStatement.setInt(21, Integer.parseInt(data[20]));
                    callableStatement.setInt(22, Integer.parseInt(data[21]));
                    callableStatement.setInt(23, Integer.parseInt(data[22]));
                    callableStatement.setInt(24,Integer.parseInt( data[23]));
                    resultSet = callableStatement.executeQuery();
                    while (resultSet.next()) {

                    }
                    resultSet.close();
                    callableStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {
                    try {
                        if(resultSet != null) {
                            resultSet.close();
                        }
                        if(callableStatement != null) {
                            callableStatement.close();
                        }
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }

        }


        public void pmdstatus(String[] data){
            ResultSet resultSet = null;
            CallableStatement callableStatement= null;

            try {


                callableStatement = this.cmysql.prepareCall("{call pmd_data_update(?,?,?,?,?,?,?,?,?,?,?,?,?)}");
                callableStatement.setInt(1, Integer.parseInt(data[0]));
                callableStatement.setInt(2, Integer.parseInt(data[1]));
                callableStatement.setInt(3, Integer.parseInt(data[2]));
                callableStatement.setInt(4, Integer.parseInt(data[3]));
                callableStatement.setInt(5, Integer.parseInt(data[4]));
                callableStatement.setInt(6, Integer.parseInt(data[5]));
                callableStatement.setInt(7, Integer.parseInt(data[6]));
                callableStatement.setInt(8, Integer.parseInt(data[7]));
                callableStatement.setInt(9, Integer.parseInt(data[8]));
                callableStatement.setInt(10, Integer.parseInt(data[9]));
                callableStatement.setInt(11, Integer.parseInt(data[10]));
                callableStatement.setInt(12, Integer.parseInt(data[11]));
                callableStatement.setInt(13, 1);
                resultSet = callableStatement.executeQuery();
                while (resultSet.next()) {

                }
                resultSet.close();
                callableStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                log1.fatal(e);
                log1.fatal("exception in mysql connection");
            }finally {
                try {
                    if(resultSet != null) {
                        resultSet.close();
                    }
                    if(callableStatement != null) {
                        callableStatement.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    log1.fatal(e);
                    log1.fatal("exception in mysql connection");
                }
            }
        }


        public void Feedbackupdate(String[] data){
            if(data.length == 4) {

                ResultSet resultSet = null;
                CallableStatement callableStatement= null;


                try {

                    callableStatement = this.cmysql.prepareCall("{call bridge_endis_status_update(?,?,?,?)}");
                    callableStatement.setInt(1, Integer.parseInt(data[0]));
                    callableStatement.setInt(2, Integer.parseInt(data[1]));
                    callableStatement.setInt(3, Integer.parseInt(data[2]));
                    callableStatement.setInt(4, Integer.parseInt(data[3]));


                    resultSet = callableStatement.executeQuery();
                    while (resultSet.next()) {

                    }
                    resultSet.close();
                    callableStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {
                    try {
                        if(resultSet != null) {
                            resultSet.close();
                        }
                        if(callableStatement != null) {
                            callableStatement.close();
                        }
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }else {

                Jedis jedis= RedisDbFactoryClient.getInstance().getConnection();

                ResultSet resultSet = null;
                CallableStatement callableStatement= null;

                try {
                    String str="";
                    for(int i=0;i<data.length;i++){
                        str=str+":"+data[i];
                    }
                    System.out.println(str);
                    callableStatement = this.cmysql.prepareCall("{call status_update(?,?,?,?,?,?,?,?,?)}");
                    callableStatement.setInt(1, Integer.parseInt(data[0]));
                    callableStatement.setInt(2, Integer.parseInt(data[1]));
                    callableStatement.setInt(3, Integer.parseInt(data[2]));
                    callableStatement.setInt(4, Integer.parseInt(data[3]));
                    callableStatement.setInt(5, Integer.parseInt(data[4]));
                    callableStatement.setInt(6, Integer.parseInt(data[5]));
                    callableStatement.setInt(7, Integer.parseInt(data[6]));
                    callableStatement.setInt(8, Integer.parseInt(data[7]));
                    callableStatement.setInt(9, Integer.parseInt(data[8]));


                    resultSet = callableStatement.executeQuery();
                    while (resultSet.next()) {
                        System.out.println(resultSet.getString("message"));

                        int sample = Integer.parseInt(resultSet.getString("status"));


                        if (sample == 2) {
                            Set<String> set = jedis.keys("{AUTH*");
                            if (set.size() > 0) {
                                List<String> values = jedis.mget(set.toArray(new String[set.size()]));
                                for (String value : values) {

                                    if (value.split(":")[8].equals(data[8])) {
                                        jedis.del(value);
                                        break;
                                    }
                                }
                            }

                        }
                    }
                    resultSet.close();
                    callableStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }finally {
                    try {
                        if(resultSet != null) {
                            resultSet.close();
                        }
                        if(callableStatement != null) {
                            callableStatement.close();
                        }
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }
        }

        public  String stringbuilder(int num,int length){
            String str=String.format("%0"+(length)+"d",0);

            return (str +Integer.toHexString(num)).substring(Integer.toHexString(num).length());
        }




    }
}
