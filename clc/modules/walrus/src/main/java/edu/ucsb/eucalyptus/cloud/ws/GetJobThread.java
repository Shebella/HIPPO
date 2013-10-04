package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.util.SbxGetJobQueue;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/2
 * Time:  1:32
 * To change this template use File | Settings | File Templates.
 */
public class GetJobThread extends Thread {
    private static Logger LOG = Logger.getLogger( GetJobThread.class );
    private static boolean bRun = true;
    private VipOperation VIP = new VipOperation(false);

    public void run() {
        while (bRun) {
            SbxRequest r = null;
            try {
                r = SbxGetJobQueue.poll();
                if (r != null) {
                    //do the get job
                    //SbxGetJobQueue.inc();
                    VIP.doTheGetJob(r);
                    //SbxGetJobQueue.dec();
                } else {
                    //SbxPutJobQueue is empty
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: GetJobThread error : " + r + "###" + t);
            }
        }//while
    }//run
}

