package com.eucalyptus.ws.util;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.handlers.ServiceSinkHandler;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/2
 * Time:  1:34
 * To change this template use File | Settings | File Templates.
 */
public class SbxGetJobQueue {
    private static Logger LOG = Logger.getLogger( SbxPutJobQueue.class );
    public static int MAXJOB = System.getProperty("euca.sbxrequest.jobget")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.jobget"));
    public static LinkedBlockingQueue<SbxRequest> queue = new LinkedBlockingQueue<SbxRequest>();

    private static final Object lock = new Object();
    private static int count = 0;
    public static void inc() {
        synchronized (lock) {
            count++;
        }
    }
    public static void dec() {
        synchronized (lock) {
            count--;
        }
    }

    public static boolean put(SbxRequest r) {
        if (r == null) return false;
        try {
            queue.put(r);
            return true;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxGetJobQueue.put error : " + r + "###" + t);
        }
        return false;
    }

    public static SbxRequest offer(SbxRequest r) {
        if (r == null) return null;
        try {
            if (queue.offer(r)) {
                return r;
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxGetJobQueue.offer error : " + r + "###" + t);
        }
        return null;
    }

    public static SbxRequest poll() {
        try {
            SbxRequest r = queue.poll();
            return r;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxGetJobQueue.poll error : " + t);
        }
        return null;
    }

    public static boolean remove(String corrid) {
        if (corrid == null) return false;
        SbxRequest r = null;
        try {
            Iterator<SbxRequest> iterator = queue.iterator();
            while(iterator.hasNext()) {
                r = iterator.next();
                if (r.userctx.getCorrelationId().equals(corrid)) {
                    iterator.remove();
                    r.clean();
                    return true;
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: SbxGetJobQueue.remove error : " + r + "###" + t);
        }
        return false;
    }

    public static boolean cleanup() {
        SbxRequest r = null;
        try {
            Iterator<SbxRequest> iterator = queue.iterator();
            while(iterator.hasNext()) {
                r = iterator.next();
                if (Contexts.bExit(r)) {
                    iterator.remove();
                    r.clean();
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: SbxGetJobQueue.cleanup error : " + r + "###" + t);
        }
        return false;
    }

    public static boolean toomanyrequests() {
        if (queue.size() >= MAXJOB) return true;
        //if (count >= MAXJOB) return true;
        return false;
    }
}
