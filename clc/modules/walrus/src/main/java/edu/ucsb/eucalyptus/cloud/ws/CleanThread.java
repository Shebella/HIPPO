package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.Contexts;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/21
 * Time:  9:50
 * To change this template use File | Settings | File Templates.
 */
public class CleanThread extends Thread {
    private static Logger LOG = Logger.getLogger( CleanThread.class );
    private static boolean bRun = true;

    public void run() {
        while (bRun) {
            try {
                TimeUnit.MINUTES.sleep(6);
                if (Contexts.SBXSVR) {
//                    Contexts.cleanup();
//                SbxRequestPool.cleanup();
//                SbxJobQueue.cleanup();
//                SbxPutJobQueue.cleanup();
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: CleanThread error : " + t);
            }
        }//while
    }//run
}
