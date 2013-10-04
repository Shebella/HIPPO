package com.eucalyptus.ws.util;

import com.eucalyptus.context.SbxRequest;
import org.apache.log4j.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/8
 * Time:  3:49
 * To change this template use File | Settings | File Templates.
 */
public class SbxJobLog {
    private static Logger LOG;
    static boolean CSSLOG; 
    static{
	CSSLOG = System.getProperty("euca.css.joblog") == null ? false : Boolean.valueOf(System.getProperty("euca.css.joblog"));
        if(CSSLOG){
            try {
                PatternLayout p=new PatternLayout();
                p.setConversionPattern("%d{yyMMdd HH:mms,SSS} %-5p (%F:%L) %m%n");
                RollingFileAppender a=null;
                a=new RollingFileAppender(p,"/var/log/eucalyptus/walrus.job.log");
                a.setMaxBackupIndex(1000);
                LOG=Logger.getLogger( SbxJobLog.class );
                LOG.removeAllAppenders();
                LOG.addAppender(a);
		        LOG.setLevel(Level.DEBUG);
            } catch (IOException e) {
                LOG=Logger.getLogger( SbxJobLog.class );
            }
        }
    }

    public static void log (String s) {
        if (!CSSLOG) return;
        if (s == null) return;
        try {
            LOG.debug(s);
        } catch (Throwable t) {
            LOG.debug(s);
        }
    }

    public static void log (SbxRequest r) {
        if (!CSSLOG) return;

        String userID = null;
        String type = null;
        Long len = 0L;
        try {
            if (r != null) {
                type = r.type;
                if (r.userctx != null && r.userctx.getUser() != null) {
                    userID = r.userctx.getUser().getName();
                }
                len = r.content_length;
            }
            LOG.debug(userID + " | " + type + " | " + len);
        } catch (Throwable t) {
            LOG.debug(userID + " | " + type + " | " + len);
        }
    }

    public static void log(SbxRequest r, String status, String msg) {
        if (!CSSLOG) return;

        String userID = null;
        String type = null;
        Long len = 0L;
        try {
            if (r != null) {
                type = r.type;
                if (r.userctx != null && r.userctx.getUser() != null) {
                    userID = r.userctx.getUser().getName();
                }
                len = r.content_length;
            }
            LOG.debug(userID + " | " + type + " | " + status + " | " + msg + " | " + len);
        } catch (Throwable t) {
            LOG.debug(userID + " | " + type + " | " + status + " | " + msg + " | " + len);
        }
    }
}