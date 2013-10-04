package com.eucalyptus.ws.util;

import com.eucalyptus.context.SbxRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/2
 * Time:  5:04
 * To change this template use File | Settings | File Templates.
 */
public class SbxPayloadLog {
    private static Logger LOG;
    static boolean CSSLOG = System.getProperty("euca.css.payloadlog") == null ? false : Boolean.valueOf(System.getProperty("euca.css.payloadlog"));
    static{
        if(CSSLOG){
            try {
                PatternLayout p=new PatternLayout();
                p.setConversionPattern("%d{yyMMdd HH:mms,SSS} %-5p (%F:%L) %m%n");
                RollingFileAppender a=null;
                a=new RollingFileAppender(p,"/var/log/eucalyptus/walrus.payload.log");
                a.setMaxBackupIndex(1000);
                LOG=Logger.getLogger( SbxPayloadLog.class );
                LOG.removeAllAppenders();
                LOG.addAppender(a);
                LOG.setLevel(Level.DEBUG);
            } catch (IOException e) {
                LOG=Logger.getLogger( SbxPayloadLog.class );
            }
        }
    }


    public static void log (SbxRequest r, String type, String status, String message, String resource) {
        if (!CSSLOG) return;

        String userID = null;
        String instID = null;
        String syncID = null;
        String reqID = null;
        String flag = null;
        try {
            if (r != null) {
                userID = r.userctx.getUser().getName();
                instID = r.msg.getMetaInstId();
                syncID = r.msg.getMetaClientType();
                reqID = r.msg.getMetaReqId();
                flag = r.msg.getMetaRetry() + "/" + r.msg.getMetaHold();
            }
            LOG.debug(userID + " | " + instID + " | " + syncID + " | " + reqID + " | " + type + " | " + status + " | " + message + " | " + resource + " | " + flag);
        } catch (Throwable t) {
            LOG.debug(userID + " | " + instID + " | " + syncID + " | " + reqID + " | " + type + " | " + status + " | " + message + " | " + resource + " | " + flag + " | " + t);
        }
    }
}
