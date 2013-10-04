package com.eucalyptus.ws.util;

import com.eucalyptus.auth.principal.User;
import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.util.WalrusProperties;
import edu.ucsb.eucalyptus.cloud.BucketLogData;
import edu.ucsb.eucalyptus.cloud.entities.BucketInfo;
import edu.ucsb.eucalyptus.msgs.AccessControlListType;
import edu.ucsb.eucalyptus.msgs.PutObjectResponseType;
import edu.ucsb.eucalyptus.util.WalrusDataMessage;
import edu.ucsb.eucalyptus.util.WalrusDataMessenger;
import org.jboss.netty.channel.Channel;

import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/14
 * Time:  3:24
 * To change this template use File | Settings | File Templates.
 */
public class SbxPutRequest extends SbxRequest {
    public static int DATAQUEUE_TIMEOUT = System.getProperty("euca.putobject.dataqueuetimeout")==null ? 7 : Integer.valueOf(System.getProperty("euca.putobject.dataqueuetimeout"));
    public static int DATAQUEUE_IDLE_TIMEOUT_MINUTES = System.getProperty("euca.putobject.idletimeoutminutes")==null ? 30 : Integer.valueOf(System.getProperty("euca.putobject.idletimeoutminutes"));
    public PutObjectResponseType reply = null;
    public Channel channel = null;
    public long syncid = -1;
    public long mtime = 0;
    public String objectName = null;
    public String versionId = WalrusProperties.NULL_VERSION_ID;
    public boolean bAddObject = false;
    public BucketLogData logData = null;
    public BucketInfo bucket = null;
    public String md5 = "";
    public Date lastModified = null;
    public AccessControlListType accessControlList = null;
    public String key = null;
    public String randomKey = null;
    public WalrusDataMessenger messenger = null;
    public String tempObjectName = null;
    public MessageDigest digest = null;
    public Object fileIO = null;
    public long size = 0;
    public WalrusDataMessage dataMessage = null;
    public boolean disconnect = false;

    public SbxPutRequest(SbxRequest req) {
        this.channelctx = req.channelctx;
        this.msg = req.msg;
        this.userctx = req.userctx;
        this.reqId = req.reqId;
        this.reqStage = req.reqStage;
        this.fake = req.fake;
        this.createtime = req.createtime;
        this.type = req.type;
        this.put = req.put;
        this.rename = req.rename;
        this.get = req.get;
        this.getdetail =req.getdetail;
        this.content_length = req.content_length;
        this.objSeq = req.objSeq;
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
            s.append("corrID=" + corrid + " | ");
            s.append("type=" + type + " | ");
            s.append("ctx=" + channelctx + " | ");
            s.append("msg=" + msg + " | ");
            s.append("ct=" + createtime + " | ");
            s.append("dis=" + disconnect);
        }catch(Throwable t){
            s.append("name=" + name + " | ");
            s.append("sbxID=" + reqId + " | ");
            s.append("corrID=" + corrid + " | ");
            s.append("type=" + type + " | ");
            s.append(t);
        }
        return s.toString();
    }

    public void updatetime() {
        try {
            if (createtime == null) {
                createtime = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
            } else {
                createtime.setTime(Calendar.getInstance().getTime().getTime());
            }
        } catch (Throwable t) {
            //NOP
        }
    }

    public boolean idletimeout() {
        try {
            Timestamp nowtime = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
            if (createtime == null) {
                createtime = nowtime;
                return false;
            }
            long diff =  nowtime.getTime() - createtime.getTime();
            if ((int)(diff / 1000) >= (DATAQUEUE_IDLE_TIMEOUT_MINUTES*60)) {
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
        reply = null;
        channel = null;
        objectName = null;
        versionId = null;
        logData = null;
        bucket = null;
        md5 = null;
        lastModified = null;
        accessControlList = null;
        key = null;
        randomKey = null;
        messenger = null;
        tempObjectName = null;
        digest = null;
        fileIO = null;
        dataMessage = null;
        fake = null;
        reqStage = null;
        createtime = null;
    }
}
