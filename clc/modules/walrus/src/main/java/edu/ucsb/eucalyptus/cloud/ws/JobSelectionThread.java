package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.handlers.SbxHold;
import com.eucalyptus.ws.handlers.ServiceSinkHandler;
import com.eucalyptus.ws.util.SbxGetJobQueue;
import com.eucalyptus.ws.util.SbxPutJobQueue;
import com.eucalyptus.ws.util.SbxPutRequest;
import edu.ucsb.eucalyptus.util.SbxJobQueue;
import edu.ucsb.eucalyptus.util.SbxRequestPool;
import org.apache.log4j.Logger;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  1:46
 * To change this template use File | Settings | File Templates.
 */
public class JobSelectionThread extends Thread {
    private static Logger LOG = Logger.getLogger( JobSelectionThread.class );
    private static SbxRequestPool sbxpool = null;
    private static boolean bRun = true;

    public JobSelectionThread(SbxRequestPool p) {
        sbxpool = p;
    }

    public void run() {
        while (bRun) {
            try {
                if (Contexts.SBXSVR) {
                    Enumeration<ArrayBlockingQueue<SbxRequest>> e = sbxpool.pool.elements();
                    boolean bSleep = true;
                    while (bRun && e.hasMoreElements()) {
                        //begin check hold flag ---
                        SbxHold.checkHOLD(1, false);
                        //end ---------------------
                        //select request
                        ArrayBlockingQueue<SbxRequest> queue = e.nextElement();
                        SbxRequest r = queue.peek();
                        if (r != null) {
                            if (r.put) {
                                SbxPutRequest req = new SbxPutRequest(r);
                                if (SbxPutJobQueue.offer(req) != null) {
                                    queue.remove(r);
                                    bSleep = false;
                                }
                                continue;
                            }
                            if (r.get) {
                                if (SbxGetJobQueue.offer(r) != null) {
                                    queue.remove(r);
                                    bSleep = false;
                                }
                                continue;
                            }
                            if (SbxJobQueue.offer(r) != null) {
                                queue.remove(r);
                                bSleep = false;
                            }
                        }
                    }
                    if (bSleep) {
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                } else {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException e) {
                bRun = false;
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: JobSelectionThread error : " + t);
            }
        }//while
    }//run
}
