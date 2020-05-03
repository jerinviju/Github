package Connectionclasses;

import Helperclasses.Constants;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class PgsqlDbFactoryClient {
    static Logger log = Logger.getLogger("dbLogger");
    private static final Object mutex = new Object();

    // JDBC Driver Name & Database URL
    static final String JDBC_DRIVER = "org.postgresql.Driver";
    static final String JDBC_DB_URL = "jdbc:postgresql://"+Constants.POST_DB_HOST+":"+Constants.POST_DB_PORT+"/"+Constants.POST_DB_NAME;


    // JDBC Database Credentials
    static final String JDBC_USER = Constants.POST_DB_USER;
    static final String JDBC_PASS = Constants.POST_DB_PASS;

    private static GenericObjectPool gPool = null;
    private static DataSource data=null;

    private static PgsqlDbFactoryClient pgsqlDbFactoryClient;

    private PgsqlDbFactoryClient(){
        try {
            data=setUpPool();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static PgsqlDbFactoryClient getInstance(){
        if(pgsqlDbFactoryClient== null){
            synchronized (mutex) {
                pgsqlDbFactoryClient = new PgsqlDbFactoryClient();
            }
        }
        return  pgsqlDbFactoryClient;
    }


    @SuppressWarnings("unused")
    public DataSource setUpPool() throws Exception {
        Class.forName(JDBC_DRIVER);

        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
        gPool = new GenericObjectPool();
        gPool.setMaxActive(5);

        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
        System.out.println(JDBC_DB_URL + ":" + JDBC_USER + ":" + JDBC_PASS);

        // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
        return new PoolingDataSource(gPool);
    }

    public Connection getConnection() {
        try {
            return data.getConnection();
        }catch (SQLException e){

        }
        return null;
    }

    public GenericObjectPool getConnectionPool() {
        return gPool;
    }

    // This Method Is Used To Print The Connection Pool Status
    public void printDbStatus() {
        String conndet = "Max.: " + getConnectionPool().getMaxActive() + "; Active: " + getConnectionPool().getNumActive() + "; Idle: " + getConnectionPool().getNumIdle();
        System.out.println(conndet);
        log.info(conndet);
    }
}
