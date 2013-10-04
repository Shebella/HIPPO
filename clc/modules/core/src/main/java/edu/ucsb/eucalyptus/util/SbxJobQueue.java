package edu.ucsb.eucalyptus.util;

import com.eucalyptus.context.Contexts;
import org.apache.log4j.Logger;
import com.eucalyptus.context.SbxRequest;
import org.jboss.netty.channel.Channel;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/5/31
 * Time:  9:34
 * To change this template use File | Settings | File Templates.
 */
public class SbxJobQueue {
    private static Logger LOG = Logger.getLogger( SbxJobQueue.class );
    public static int MAXJOB = System.getProperty("euca.sbxrequest.jobsize")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.jobsize"));
    private static ArrayBlockingQueue<SbxRequest> queue = new ArrayBlockingQueue<SbxRequest>(MAXJOB, true);

    public static SbxRequest offer(SbxRequest r) {
        if (r == null) return null;
        try {
            if (queue.offer(r)) {
                return r;
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxJobQueue.offer error : " + r + "###" + t);
        }
        return null;
    }

    public static SbxRequest poll() {
        try {
            SbxRequest r = queue.poll(100, TimeUnit.MILLISECONDS);
            return r;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxJobQueue.poll error : " + t);
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
            //LOG.debug("THROTTLINGandRETRY: SbxJobQueue.remove error : " + r + "###" + t);
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
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: SbxJobQueue.remove error : " + r + "###" + t);
        }
        return false;
    }
}
