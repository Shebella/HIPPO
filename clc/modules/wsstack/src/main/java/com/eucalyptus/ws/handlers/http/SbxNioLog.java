package com.eucalyptus.ws.handlers.http;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/23
 * Time:  1:00
 * To change this template use File | Settings | File Templates.
 */
public class SbxNioLog {
    private static Logger LOG = Logger.getLogger( SbxNioLog.class );
    static boolean CSSLOG = System.getProperty("euca.css.niolog") == null ? false : Boolean.valueOf(System.getProperty("euca.css.niolog"));
    static{
        if(CSSLOG){
            try {
                PatternLayout p=new PatternLayout();
                p.setConversionPattern("%d{yyMMdd HH:mms,SSS} %-5p (%F:%L) %m%n");
                RollingFileAppender a=null;
                a=new RollingFileAppender(p,"/var/log/eucalyptus/walrus.nio.log");
                a.setMaxBackupIndex(1000);
                LOG.removeAllAppenders();
                LOG.addAppender(a);
                LOG.setLevel(Level.DEBUG);
            } catch (Throwable t) {
                //NOP
            }
        }
    }

    public static void log (String url) {
        if (!CSSLOG) return;

        try {
            LOG.debug(url);
        } catch (Throwable t) {
            LOG.debug(url + " | " + t);
        }
    }
}
