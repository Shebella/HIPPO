package edu.ucsb.eucalyptus.util;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import edu.ucsb.eucalyptus.cloud.CSSException;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/5/30
 * Time:  1:57
 * To change this template use File | Settings | File Templates.
 */
public class SbxRequestPool {
    private static Logger LOG = Logger.getLogger( SbxRequestPool.class );
    private static int MAXUSER = 100;
    private static int CONCURR = MAXUSER / ( Runtime.getRuntime().availableProcessors() * 2 + 1 );
    private static float THRESHOLD = 1.0f;
    public static int MAXREQ = System.getProperty("euca.sbxrequest.queuesize")==null ? 40 : Integer.valueOf(System.getProperty("euca.sbxrequest.queuesize"));
    public static ConcurrentHashMap<String, ArrayBlockingQueue<SbxRequest>> pool = new ConcurrentHashMap<String, ArrayBlockingQueue<SbxRequest>>(MAXUSER, THRESHOLD, CONCURR);
    private static final Object accountLock = new Object();

    public static ArrayBlockingQueue<SbxRequest> lookup(String userName) {
        try {
            ArrayBlockingQueue<SbxRequest> queue = null;
            if (!pool.containsKey(userName)) {
                synchronized (accountLock) {
                    queue = pool.get(userName);
                    if (queue == null) {
                        queue = new ArrayBlockingQueue<SbxRequest>(MAXREQ, true);
                        pool.put(userName, queue);
                    }
                }
            } else {
                queue = pool.get(userName);
            }
            return queue;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxRequestPool.lookup error : " + t);
        }
        return null;
    }

    public static SbxRequest lookup(String userName, String reqId) {
        try {
            ArrayBlockingQueue<SbxRequest> queue = lookup(userName);
            SbxRequest[] array = (SbxRequest[]) queue.toArray();
            for (int i=0; i<array.length; i++) {
                if (array[i].reqId.equals(reqId)) {
                    return array[i];
                }
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxRequestPool.lookup(reqId) error : " + t);
        }
        return null;
    }

    public static SbxRequest offer(String userName, SbxRequest r) {
        try {
            ArrayBlockingQueue<SbxRequest> queue = lookup(userName);
            assert queue != null : "THROTTLINGandRETRY: SbxRequestPool.offer error : lookup is null";
            if (queue == null) {
                throw new CSSException("THROTTLINGandRETRY: SbxRequestPool.offer error : lookup is null");
            }
            if (queue.offer(r)) {
                return r;
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxRequestPool.offer error : " + r + "###" + t);
        }
        return null;
    }

    public static boolean remove(String userName, String corrid) {
        if (corrid == null) return false;
        if (userName == null) {
            //LOG.debug("THROTTLINGandRETRY: SbxRequestPool.remove error : userName is null. corrid : " + corrid);
            return false;
        }
        SbxRequest r = null;
        try {
            ArrayBlockingQueue<SbxRequest> queue = lookup(userName);
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
            //LOG.debug("THROTTLINGandRETRY: SbxRequestPool.remove error : " + r + "###" + t);
        }
        return false;
    }

    public static void cleanup() {
        String account = null;
        try {
            Iterator iterator = pool.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                account = (String) entry.getKey();
                ArrayBlockingQueue<SbxRequest> queue = (ArrayBlockingQueue<SbxRequest>) entry.getValue();
                if (queue != null) {
                    Iterator<SbxRequest> ptr = queue.iterator();
                    while (ptr.hasNext()) {
                        SbxRequest r = ptr.next();
                        if (Contexts.bExit(r)) {
                            ptr.remove();
                            r.clean();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxRequestPool.cleanup error : " + account + " : " + t);
        }
    }

    public static int size() {
        int size = 0;
        Iterator iterator = pool.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //String account = (String) entry.getKey();
            ArrayBlockingQueue<SbxRequest> queue = (ArrayBlockingQueue<SbxRequest>) entry.getValue();
            if (queue != null && queue.size() > 0) {
                size++;
            }
        }
        if (size <= 0) return 1;
        return size;
    }
}
