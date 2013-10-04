package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.ws.util.SbxJobLog;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/8
 * Time:  3:54
 * To change this template use File | Settings | File Templates.
 */
public class SbxJobLogThread extends Thread {
    private static Logger LOG = Logger.getLogger( SbxJobLogThread.class );
    private static boolean bRun = true;

    public void run() {
        while (bRun) {
            try {
                TimeUnit.MINUTES.sleep(10);
                SbxJobLog.log("------");
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: SbxJobLogThread error : " + t);
            }
        }//while
    }//run
}