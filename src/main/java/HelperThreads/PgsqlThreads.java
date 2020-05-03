package HelperThreads;

import Connectionclasses.PgsqlDbFactoryClient;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PgsqlThreads implements Runnable {


    public BlockingQueue<String> PgsqldataQueue;
    ExecutorService executor =null;
    static PgsqlThreads pgsqlThreads=null;
    private static final Object mutex = new Object();

    private PgsqlThreads(){
        try {
            executor= Executors.newFixedThreadPool(5);
            PgsqldataQueue = new ArrayBlockingQueue<>(500000);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static PgsqlThreads getinstance(){
        if(pgsqlThreads==null){
            synchronized (mutex) {
                pgsqlThreads = new PgsqlThreads();
            }
        }
        return pgsqlThreads;
    }

    @Override
    public void run() {
        while(true){
            try{
                List<String> sqlStrings = new ArrayList<>();
                PgsqldataQueue.drainTo(sqlStrings);
                if(sqlStrings.size()>0) {
                    Task task = new Task(PgsqlDbFactoryClient.getInstance().getConnection(), sqlStrings);
                    executor.execute(task);
                }
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    class Task implements  Runnable{

        Connection cpost;
        Statement stmt;
        List<String> sqlStrings;

        private Task(Connection cpost,List<String> sqlStrings){
            this.cpost=cpost;
            this.sqlStrings=sqlStrings;
        }

        @Override
        public void run() {
            try {
                if(cpost!=null) {
                    stmt = cpost.createStatement();
                    for (String sql : sqlStrings
                    ) {
                        stmt.addBatch(sql);
                    }

                    stmt.executeBatch();
                    stmt.close();
                }else{
                    for (String sql : sqlStrings
                    ) {
                        PgsqldataQueue.put(sql);
                    }
                }
            }catch (Exception e){

            }

        }

    }

}
