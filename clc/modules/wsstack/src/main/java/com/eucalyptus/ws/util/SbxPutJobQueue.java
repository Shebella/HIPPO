package com.eucalyptus.ws.util;

import com.eucalyptus.context.Contexts;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/14
 * Time:  3:03
 * To change this template use File | Settings | File Templates.
 */
public class SbxPutJobQueue {
    private static Logger LOG = Logger.getLogger( SbxPutJobQueue.class );
    public static int MAXJOB = System.getProperty("euca.sbxrequest.jobput")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.jobput"));
    private static LinkedBlockingQueue<SbxPutRequest> queue = new LinkedBlockingQueue<SbxPutRequest>();

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


    public static boolean put(SbxPutRequest r) {
        if (r == null) return false;
        try {
            queue.put(r);
            return true;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxPutJobQueue.put error : " + r + "###" + t);
        }
        return false;
    }

    public static SbxPutRequest offer(SbxPutRequest r) {
        if (r == null) return null;
        try {
            if (queue.offer(r)) {
                return r;
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxPutJobQueue.offer error : " + r + "###" + t);
        }
        return null;
    }

    public static SbxPutRequest poll() {
        try {
            SbxPutRequest r = queue.poll(100, TimeUnit.MILLISECONDS);
            return r;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxPutJobQueue.poll error : " + t);
        }
        return null;
    }

    public static boolean remove(String corrid) {
        if (corrid == null) return false;
        SbxPutRequest r = null;
        try {
            Iterator<SbxPutRequest> iterator = queue.iterator();
            while(iterator.hasNext()) {
                r = iterator.next();
                if (r.userctx.getCorrelationId().equals(corrid)) {
                    iterator.remove();
                    r.clean();
                    return true;
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: SbxPutJobQueue.remove error : " + r + "###" + t);
        }
        return false;
    }

    public static boolean cleanup() {
        SbxPutRequest r = null;
        try {
            Iterator<SbxPutRequest> iterator = queue.iterator();
            while(iterator.hasNext()) {
                r = iterator.next();
                if (Contexts.bExit(r)) {
                    iterator.remove();
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: SbxPutJobQueue.cleanup error : " + r + "###" + t);
        }
        return false;
    }

    public static boolean toomanyrequests() {
        if (queue.size() >= MAXJOB) return true;
        //if (count >= MAXJOB) return true;
        return false;
    }
}
