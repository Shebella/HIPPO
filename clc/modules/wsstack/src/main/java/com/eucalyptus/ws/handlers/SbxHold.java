package com.eucalyptus.ws.handlers;

import com.eucalyptus.context.Contexts;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/25
 * Time:  2:04
 * To change this template use File | Settings | File Templates.
 */
public class SbxHold {
    private static Logger LOG = Logger.getLogger( SbxHold.class );
    private static int hold = 0;
    private static final Object holdLock = new Object();
    public final static String HOLD1="hold1",UNHOLD1="unhold1",HOLD2="hold2",UNHOLD2="unhold2",HOLD3="hold3",UNHOLD3="unhold3",UNHOLD="unhold";

    public static void setHold(String h) {
        if (h == null) return;
        if (h.isEmpty()) return;
        if (h.equals("VIP")) return;
        synchronized (holdLock) {
            try {
                if (h.equals(HOLD1)) {
                    hold |= 0x1;
                } else if (h.equals(HOLD2)) {
                    hold |= 0x1<<1;
                } else if (h.equals(HOLD3)) {
                    hold |= 0x1<<2;
                } else if (h.equals(UNHOLD1)) {
                    int mask = ~(0x01);
                    hold &= mask;
                } else if (h.equals(UNHOLD2)) {
                    int mask = ~(0x01<<1);
                    hold &= mask;
                } else if (h.equals(UNHOLD3)) {
                    int mask = ~(0x01<<2);
                    hold &= mask;
                } else if (h.equals(UNHOLD)) {
                    hold = 0x0;
                }
            } catch (Throwable t) {
                LOG.debug("THROTTLINGandRETRY: SbxHold.setHold error : " + t);
            }
        }
    }

    public static boolean isHOLD(int shift) {
        int mask = 0x01;
        for (int i=1; i<shift; i++) mask <<= 1;
        if ((hold & mask) != 0) { // don't care about race condition of "hold"
            return true;
        }
        return false;
    }

    public static void checkHOLD(int shift, boolean isVIP) {
        if (Contexts.SBXSVR) { // disable hold for debugging
            if (true) return;
        }
        try {
            if (isVIP) return;
            //check hold flag
            while (isHOLD(shift)) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (Throwable t) {
            //NOP
        }
    }
}
