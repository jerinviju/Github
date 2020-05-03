package HelperThreads;

import org.apache.log4j.Logger;

import java.io.File;

import static Helperclasses.Constants.*;


public class Logchecker extends  Thread {

    static Logger log2 = Logger.getLogger("exceptionLogger");


    @Override
    public void run() {
        super.run();


            while (true) {
                try {
                    File[] roots = File.listRoots();

                    /* For each filesystem root, print some info */
                    for (File root : roots) {


                        if (root.getUsableSpace() < 2961212928l) {
                            FEEDBACK_LOG = 0;
                            RFID_LOG = 0;
                            SENSOR_LOG = 0;
                            POWER_LOG = 0;
                            STATUS_LOG = 0;
                            VOLTAGE_LOG = 0;
                            USB_STATUS_LOG = 0;
                            BRIDGE_COMMAND_LOG = 0;
                            DEVICE_VERSION_LOG = 0;
                        } else if (FEEDBACK_LOG == 0 || RFID_LOG == 0 || SENSOR_LOG == 0 || POWER_LOG == 0 || STATUS_LOG == 0 || VOLTAGE_LOG == 0 || USB_STATUS_LOG == 0 || BRIDGE_COMMAND_LOG == 0 || DEVICE_VERSION_LOG == 0) {
                            FEEDBACK_LOG = 1;
                            RFID_LOG = 1;
                            SENSOR_LOG = 1;
                            POWER_LOG = 1;
                            STATUS_LOG = 1;
                            VOLTAGE_LOG = 1;
                            USB_STATUS_LOG = 1;
                            BRIDGE_COMMAND_LOG = 1;
                            DEVICE_VERSION_LOG = 1;
                        }
                    }

                    try {
                        Thread.sleep(1000*60);
                    } catch (Exception e) {
                        e.printStackTrace();
                        log2.fatal("exception in log check" + e);
                    }
                }catch ( Exception e){
                    e.printStackTrace();
                    log2.fatal("exception in log check" + e);
                }
            }


    }
}
