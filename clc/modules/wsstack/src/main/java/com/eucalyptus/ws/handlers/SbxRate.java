package com.eucalyptus.ws.handlers;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/25
 * Time:  2:14
 * To change this template use File | Settings | File Templates.
 */
public class SbxRate {
    private static Logger LOG = Logger.getLogger( SbxRate.class );
    public static int MAXREQ = System.getProperty("euca.sbxrequest.queuesize")==null ? 40 : Integer.valueOf(System.getProperty("euca.sbxrequest.queuesize"));
    private static int EmptyCount = 0;
    private static final Object countLock = new Object();
    private static int EmptyRateCount = 0;
    private static float EmptyRateAvg = 0.0f;
    private static int EmptyRate = 0;
    private static int size = 0;

    public static void inc() {
        //synchronized (countLock) {
            EmptyCount++;
        //}
    }

    public static int zero() {
        //synchronized (countLock) {
            int count = EmptyCount;
            EmptyCount = 0;
            return count;
        //}
    }

    public static void setRate(int avarage) {
        int s = size;
        if (avarage <= 0) {
            EmptyRate = 1;
        } else {
            if (s <= 0)
                EmptyRate = avarage;
            else
                EmptyRate = avarage / s;
        }
    }

    public static String getRate() {
        int rate = EmptyRate;
        if (rate <= 0) {
            return String.valueOf(1);
        }
        if (rate > MAXREQ) {
            return String.valueOf(MAXREQ);
        }
        return String.valueOf(rate);
    }

    public static void calculateRate(int duration) {
        try {
            EmptyRateCount++;
            float rate = (float)zero() / (float)duration;
            if (rate > 0.0f) {
                EmptyRateAvg = (EmptyRateAvg*(float)(EmptyRateCount-1) + rate) / (EmptyRateCount);
                setRate((int)(EmptyRateAvg));
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread.calculateRate error : " + t);
        }
    }
}
