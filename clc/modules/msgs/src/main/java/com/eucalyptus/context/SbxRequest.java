package com.eucalyptus.context;

import com.eucalyptus.auth.principal.User;
import edu.ucsb.eucalyptus.msgs.BaseMessage;
import org.jboss.netty.channel.ChannelHandlerContext;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.Calendar;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/7
 * Time: 2:44
 * To change this template use File | Settings | File Templates.
 */
public class SbxRequest {
    public static int TIMEOUT = System.getProperty("euca.sbxrequest.timeout")==null ? 20 : Integer.valueOf(System.getProperty("euca.sbxrequest.timeoutminutes"));
    public ChannelHandlerContext channelctx = null;
    public BaseMessage msg = null;
    public Context userctx = null;
    public String reqId = null;
    public static enum STAGE { START, S0, S1, S2, S3, UPDATE, FINAL };
    public STAGE reqStage = STAGE.START;
    public String fake = null;
    public Timestamp createtime = null;
    public String type = null;
    public boolean put = false;
    public boolean rename = false;
    public boolean get = false;
    public boolean getdetail = false;
    public long content_length = 0;
    public String objSeq = null;

    public SbxRequest() {
        createtime = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
    }

    @Override
    public String toString() {
        StringBuffer s = new StringBuffer();
        String name = null;
        String corrid = null;
        try {
            if (userctx != null){
                User u = userctx.getUser();
                if (u != null) {
                    name = u.getName();
                }
                corrid = userctx.getCorrelationId();
            }
            s.append("name=" + name + " | ");
            s.append("sbxID=" + reqId + " | ");
            s.append("type=" + type + " | ");
            s.append("corrID=" + corrid + " | ");
            s.append("ctx=" + channelctx + " | ");
            s.append("msg=" + msg + " | ");
            s.append("ct=" + createtime);
        }catch(Throwable t){
            s.append("name=" + name + " | ");
            s.append("sbxID=" + reqId + " | ");
            s.append(t);
        }
        return s.toString();
    }

    public boolean timeout() {
        try {
            Timestamp nowtime = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
            long diff =  nowtime.getTime() - createtime.getTime();
            if ((int)(diff / 1000) >= (TIMEOUT*60)) {
                return true;
            }
        } catch (Throwable t) {
            //NOP
        }
        return false;
    }

    public void clean() {
        channelctx = null;
        msg = null;
        userctx = null;
        reqId = null;
        fake = null;
        reqStage = null;
        createtime = null;
        type = null;
    }
}
