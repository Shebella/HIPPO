package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.SbxRequest;
import edu.ucsb.eucalyptus.util.SbxJobQueue;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  1:41
 * To change this template use File | Settings | File Templates.
 */
public class JobThread extends Thread  {
    private static Logger LOG = Logger.getLogger( JobThread.class );
    private static boolean bRun = true;
    private VipOperation VIP = new VipOperation(false);

    public void run() {
        while (bRun) {
            SbxRequest r = null;
            try {
                r = SbxJobQueue.poll();
                if (r != null) {
                    //do the job
                    VIP.doJob(r);
                } else {
                    //jobqueue is empty
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: JobThread error : " + r + "###" + t);
            }
        }//while
    }//run
}
