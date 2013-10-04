package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.ws.util.*;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  1:42
 * To change this template use File | Settings | File Templates.
 */
public class PutJobThread extends Thread {
    private static Logger LOG = Logger.getLogger( PutJobThread.class );
    private static boolean bRun = true;
    private VipOperation VIP = new VipOperation(false);

    public void run() {
        while (bRun) {
            SbxPutRequest r = null;
            try {
                r = SbxPutJobQueue.poll();
                if (r != null) {
                    //do the put job
                    //SbxPutJobQueue.inc();
                    VIP.doThePutJob(r);
                    //SbxPutJobQueue.dec();
                } else {
                    //SbxPutJobQueue is empty
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: PutJobThread error : " + r + "###" + t);
            }
        }//while
    }//run
}
