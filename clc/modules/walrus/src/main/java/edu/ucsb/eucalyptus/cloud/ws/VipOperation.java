package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.handlers.SbxHold;
import com.eucalyptus.ws.handlers.ServiceSinkHandler;
import com.eucalyptus.ws.util.*;
import edu.ucsb.eucalyptus.cloud.AccessDeniedException;
import edu.ucsb.eucalyptus.cloud.S3Exception;
import edu.ucsb.eucalyptus.msgs.*;
import edu.ucsb.eucalyptus.util.FakeResponse;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  1:42
 * To change this template use File | Settings | File Templates.
 */
public class VipOperation implements Runnable {
    private static Logger LOG = Logger.getLogger( VipOperation.class );
    private SbxRequest req = null;
    private boolean isVIP = false;

    public VipOperation(boolean VIP) {
        isVIP = VIP;
    }

    public void init(boolean VIP, SbxRequest r) {
        isVIP = VIP;
        req = r;
    }

    private boolean checkFakeResponse(FakeResponse.S3METHOD method, SbxRequest req) {
        try {
            if (FakeResponse.isFakeMode) {
                S3Exception e = FakeResponse.check(method);
                if (e != null) {
                    ServiceSinkHandler.handleException(e, req);
                    return true;
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: checkFakeResponse error | " + req + " | " + t);
        }
        return false;
    }


    private static final Object wmLock = new Object();
    public void doJob(SbxRequest req) {
        try {
            if (WalrusControl.walrusManager == null) {
                synchronized (wmLock) {
                    if (WalrusControl.walrusManager == null) {
                        WalrusControl.configure();
                    }
                }
            }
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: JobThread.doJob error : WalrusControl.configure error : " + req + " : " + t);
        }

        try {
            if(req.msg instanceof PutObjectType){
                if(((PutObjectType)req.msg).getRenameto()!=null){
                    SbxHold.checkHOLD(2, isVIP);
                    if (checkFakeResponse(FakeResponse.S3METHOD.RENAMEOBJECT, req)) return;
                    if (Contexts.SBXSVR) {
                        String account = ((PutObjectType) req.msg).getUserId();
                        String instid = ((PutObjectType) req.msg).getMetaInstId();
                        WalrusControl.walrusManager.callWalrusHeartBeat(account, instid, "RENAMEOBJECT");
                        //set generation number
                        req.objSeq = ((PutObjectType)req.msg).getMetaObjSeq();
                    }
                    Object r = WalrusControl.walrusManager.renameObject((PutObjectType) req.msg, req);
                    SbxHold.checkHOLD(3, isVIP);
                    ServiceSinkHandler.handle((BaseMessage)r, req);
                } else {
                    doPutJob(req);
                    return;
                }
            }else if(req.msg instanceof GetObjectType){
                doGetJob(req);
                return;
            }else if(req.msg instanceof DeleteObjectType){
                SbxHold.checkHOLD(2, isVIP);
                if (checkFakeResponse(FakeResponse.S3METHOD.DELETEOBJECT, req)) return;
                if (Contexts.SBXSVR) {
                    String account = ((DeleteObjectType) req.msg).getUserId();
                    String instid = ((DeleteObjectType) req.msg).getMetaInstId();
                    WalrusControl.walrusManager.callWalrusHeartBeat(account, instid, "RENAMEOBJECT");
                    //set generation number
                    req.objSeq = ((DeleteObjectType)req.msg).getMetaObjSeq();
                }
                Object r = WalrusControl.walrusManager.deleteObject((DeleteObjectType) req.msg);
                SbxHold.checkHOLD(3, isVIP);
                ServiceSinkHandler.handle((BaseMessage)r, req);
            }else if(req.msg instanceof ListAllMyBucketsType){
                SbxHold.checkHOLD(2, isVIP);
                if (checkFakeResponse(FakeResponse.S3METHOD.LISTALLMYBUCKET, req)) return;
                Object r = WalrusControl.walrusManager.listAllMyBuckets((ListAllMyBucketsType) req.msg);
                SbxHold.checkHOLD(3, isVIP);
                ServiceSinkHandler.handle((BaseMessage)r, req);
            }else if(req.msg instanceof CreateBucketType){
                SbxHold.checkHOLD(2, isVIP);
                if (checkFakeResponse(FakeResponse.S3METHOD.CREATEBUCKET, req)) return;
                Object r = WalrusControl.walrusManager.createBucket((CreateBucketType) req.msg);
                SbxHold.checkHOLD(3, isVIP);
                ServiceSinkHandler.handle((BaseMessage)r, req);
            }else if(req.msg instanceof DeleteBucketType){
                SbxHold.checkHOLD(2, isVIP);
                if (checkFakeResponse(FakeResponse.S3METHOD.DELETEBUCKET, req)) return;
                Object r = WalrusControl.walrusManager.deleteBucket((DeleteBucketType) req.msg);
                SbxHold.checkHOLD(3, isVIP);
                ServiceSinkHandler.handle((BaseMessage)r, req);
            }else if(req.msg instanceof ListBucketType){
                SbxHold.checkHOLD(2, isVIP);
                if (checkFakeResponse(FakeResponse.S3METHOD.LISTBUCKET, req)) return;
                Object r = WalrusControl.walrusManager.listBucket((ListBucketType) req.msg);
                SbxHold.checkHOLD(3, isVIP);
                ServiceSinkHandler.handle((BaseMessage)r, req);
            }else {
                LOG.debug("THROTTLINGandRETRY: JobThread.doJob error : not support : " + req);
                Throwable t =  new AccessDeniedException("CssError", "doJob: not support", true);
                ServiceSinkHandler.handleException(t, req);
            }
        }catch(Throwable t){
            ServiceSinkHandler.handleException(t, req);
        }
    }//doJob

    public void doPutJob(SbxRequest req) {
        String type = "PutObject";
        try {
            SbxPutRequest r = new SbxPutRequest(req);
            SbxPutJobQueue.put(r);
        } catch (Throwable t) {
            // Should not catch any exception
            LOG.debug("THROTTLINGandRETRY: doPutJob error | " + req + " | " + t);
            ServiceSinkHandler.handleException(t, req);
        }
    }//doPutJob

    public void doThePutJob(SbxPutRequest req) {
        String type = "PutObject";
        try {
            SbxHold.checkHOLD(2, isVIP);
            if (checkFakeResponse(FakeResponse.S3METHOD.PUTOBJECT, req)) return;
            if (Contexts.SBXSVR) {
                String account = ((PutObjectType) req.msg).getUserId();
                String instid = ((PutObjectType) req.msg).getMetaInstId();
                WalrusControl.walrusManager.callWalrusHeartBeat(account, instid, "PUTOBJECT");
                //set generation number
                req.objSeq = ((PutObjectType)req.msg).getMetaObjSeq();
            }
            BaseMessage r = WalrusControl.walrusManager.putObject((PutObjectType) req.msg, req);
            SbxHold.checkHOLD(3, isVIP);
            if (req.reqStage == SbxPutRequest.STAGE.FINAL) {
                ServiceSinkHandler.handle(r, req);
            }
        }catch(Throwable t){
            ServiceSinkHandler.handleException(t, req);
        }
    }//doThePutJob

    public void doGetJob(SbxRequest req) {
        try {
            SbxGetJobQueue.put(req);
        } catch (Throwable t) {
            // Should not catch any exception
            LOG.debug("THROTTLINGandRETRY: doGetJob error | " + req + " | " + t);
            ServiceSinkHandler.handleException(t, req);
        }
    }//doGetJob

    public void doTheGetJob(SbxRequest req) {
        try {
            SbxHold.checkHOLD(2, isVIP);
            if (checkFakeResponse(FakeResponse.S3METHOD.GETOBJECT, req)) return;
            if (Contexts.SBXSVR) {
                String account = ((GetObjectType) req.msg).getUserId();
                String instid = ((GetObjectType) req.msg).getMetaInstId();
                WalrusControl.walrusManager.callWalrusHeartBeat(account, instid, "GETOBJECT");
                //set generation number
                req.objSeq = ((GetObjectType)req.msg).getMetaObjSeq();
            }
            Object r = WalrusControl.walrusManager.getObject((GetObjectType) req.msg, req);
            SbxHold.checkHOLD(3, isVIP);
            ServiceSinkHandler.handle((BaseMessage)r, req);
        }catch(Throwable t){
            ServiceSinkHandler.handleException(t, req);
        }
    }//doTheGetJob

    public void run() {
        if (req != null) {
            doJob(req);
        }
    }
}
