package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.handlers.ServiceSinkHandler;
import com.eucalyptus.ws.util.SbxGetJobQueue;
import edu.ucsb.eucalyptus.util.SbxRequestPool;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/2
 * Time:  2:35
 * To change this template use File | Settings | File Templates.
 */
public class TimeoutThread extends Thread {
    private static Logger LOG = Logger.getLogger( TimeoutThread.class );
    private static boolean bRun = true;

    public void run() {
        while (bRun) {
            try {
                TimeUnit.MINUTES.sleep(2);
                if (Contexts.SBXSVR) {
//                    SbxGetJobQueueTimeout();
//                    SbxRequestPoolTimeout();
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: TimeoutThread error : " + t);
            }
        }//while
    }//run

    public static void SbxRequestPoolTimeout() {
        String account = null;
        SbxRequest r = null;
        try {
            Iterator iterator = SbxRequestPool.pool.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                account = (String) entry.getKey();
                ArrayBlockingQueue<SbxRequest> queue = (ArrayBlockingQueue<SbxRequest>) entry.getValue();
                if (queue != null) {
                    Iterator<SbxRequest> ptr = queue.iterator();
                    while (ptr.hasNext()) {
                        r = ptr.next();
                        if (r.timeout()) {
                            ptr.remove();
                            ServiceSinkHandler.ServerTooBusy(r, true);
                            r.clean();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxRequestPoolTimeout error : " + account + " : " + r + " : " + t);
        }
    }

    public static void SbxGetJobQueueTimeout() {
        SbxRequest r = null;
        try {
            Iterator<SbxRequest> iterator = SbxGetJobQueue.queue.iterator();
            while(iterator.hasNext()) {
                r = iterator.next();
                if (r.timeout()) {
                    iterator.remove();
                    ServiceSinkHandler.ServerTooBusy(r);
                    r.clean();
                }
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxGetJobQueueTimeout error : " + r + " : " + t);
        }
    }
}
