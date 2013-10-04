package edu.ucsb.eucalyptus.util;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.SbxRequest;
import edu.ucsb.eucalyptus.cloud.S3Exception;
import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  1:06
 * To change this template use File | Settings | File Templates.
 */
public class FakeResponse {
    private static Logger LOG = Logger.getLogger( FakeResponse.class );
    public static boolean isFakeMode = System.getProperty("euca.sbxrequest.fake")==null ? false : Boolean.valueOf(System.getProperty("euca.sbxrequest.fake"));
    public static enum S3METHOD { LISTALLMYBUCKET,CREATEBUCKET, DELETEBUCKET, LISTBUCKET, PUTOBJECT, GETOBJECT, RENAMEOBJECT, DELETEOBJECT };
    public static enum S3CODE {
        S000,S001,
        S100,S101,S102,S103,S104,S105,
        S200,S201,S202,S203,
        S300,S301,S302,
        S400,S401,S402,S403,S404,S405,S406,S407,
        S500,S501,S502,S503,S504,S505,S506,S507,S508,
        S600,S601,S602,S603,S604,S605,S606,
        S700,S701,S702,S703,S704,
        S999
    };
    private static String path = "/tmp/fakeresponse.cfg";

    public static S3METHOD getMethod(String method) {
        if (method == null) return null;
        S3METHOD s3m = null;
        if (method.equals("LISTALLMYBUCKET")) {
            s3m = S3METHOD.LISTALLMYBUCKET;
        } else if (method.equals("CREATEBUCKET")) {
            s3m = S3METHOD.CREATEBUCKET;
        } else if (method.equals("DELETEBUCKET")) {
            s3m = S3METHOD.DELETEBUCKET;
        } else if (method.equals("LISTBUCKET")) {
            s3m = S3METHOD.LISTBUCKET;
        } else if (method.equals("PUTOBJECT")) {
            s3m = S3METHOD.PUTOBJECT;
        } else if (method.equals("GETOBJECT")) {
            s3m = S3METHOD.GETOBJECT;
        } else if (method.equals("RENAMEOBJECT")) {
            s3m = S3METHOD.RENAMEOBJECT;
        } else if (method.equals("DELETEOBJECT")) {
            s3m = S3METHOD.DELETEOBJECT;
        }
        return s3m;
    }//getMethod

    public static S3CODE getCode(String error) {
        if (error == null) return null;
        S3CODE s3c = null;
        if (error.equals("000")) {
            s3c = S3CODE.S000;
        } else if (error.equals("001")) {
            s3c = S3CODE.S001;
        } else if (error.equals("100")) {
            s3c = S3CODE.S100;
        } else if (error.equals("101")) {
            s3c = S3CODE.S101;
        } else if (error.equals("102")) {
            s3c = S3CODE.S102;
        } else if (error.equals("103")) {
            s3c = S3CODE.S103;
        } else if (error.equals("104")) {
            s3c = S3CODE.S104;
        } else if (error.equals("105")) {
            s3c = S3CODE.S105;
        } else if (error.equals("200")) {
            s3c = S3CODE.S200;
        } else if (error.equals("201")) {
            s3c = S3CODE.S201;
        } else if (error.equals("202")) {
            s3c = S3CODE.S202;
        } else if (error.equals("203")) {
            s3c = S3CODE.S203;
        } else if (error.equals("300")) {
            s3c = S3CODE.S300;
        } else if (error.equals("301")) {
            s3c = S3CODE.S301;
        } else if (error.equals("302")) {
            s3c = S3CODE.S302;
        } else if (error.equals("400")) {
            s3c = S3CODE.S400;
        } else if (error.equals("401")) {
            s3c = S3CODE.S401;
        } else if (error.equals("402")) {
            s3c = S3CODE.S402;
        } else if (error.equals("403")) {
            s3c = S3CODE.S403;
        } else if (error.equals("404")) {
            s3c = S3CODE.S404;
        } else if (error.equals("405")) {
            s3c = S3CODE.S405;
        } else if (error.equals("406")) {
            s3c = S3CODE.S406;
        } else if (error.equals("407")) {
            s3c = S3CODE.S407;
        } else if (error.equals("500")) {
            s3c = S3CODE.S500;
        } else if (error.equals("501")) {
            s3c = S3CODE.S501;
        } else if (error.equals("502")) {
            s3c = S3CODE.S502;
        } else if (error.equals("503")) {
            s3c = S3CODE.S503;
        } else if (error.equals("504")) {
            s3c = S3CODE.S504;
        } else if (error.equals("505")) {
            s3c = S3CODE.S505;
        } else if (error.equals("506")) {
            s3c = S3CODE.S506;
        } else if (error.equals("507")) {
            s3c = S3CODE.S507;
        } else if (error.equals("508")) {
            s3c = S3CODE.S508;
        } else if (error.equals("600")) {
            s3c = S3CODE.S600;
        } else if (error.equals("601")) {
            s3c = S3CODE.S601;
        } else if (error.equals("602")) {
            s3c = S3CODE.S602;
        } else if (error.equals("603")) {
            s3c = S3CODE.S603;
        } else if (error.equals("604")) {
            s3c = S3CODE.S604;
        } else if (error.equals("605")) {
            s3c = S3CODE.S605;
        } else if (error.equals("606")) {
            s3c = S3CODE.S606;
        } else if (error.equals("700")) {
            s3c = S3CODE.S700;
        } else if (error.equals("701")) {
            s3c = S3CODE.S701;
        } else if (error.equals("702")) {
            s3c = S3CODE.S702;
        } else if (error.equals("703")) {
            s3c = S3CODE.S703;
        } else if (error.equals("704")) {
            s3c = S3CODE.S704;
        } else if (error.equals("999")) {
            s3c = S3CODE.S999;
        }
        return s3c;
    }//getCode

    public static S3Exception toS3Exception(S3CODE e) {
        if (e == null) return null;
        if (e == S3CODE.S000) {
            return new S3Exception("AccessDenied", "000: No such user", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S001) {
            return new S3Exception("AccessDenied", "001: User not found", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S100) {
            return new S3Exception("AccessDenied", "100: userID is null", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S101) {
            return new S3Exception("AccessDenied", "101: Unable to create bucket", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S102) {
            return new S3Exception("AccessDenied", "102: Could not get Walrus URL", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S103) {
            return new S3Exception("Conflict", "103: Bucket already owned by you", "Bucket", null, HttpResponseStatus.CONFLICT, true);
        } else if (e == S3CODE.S104) {
            return new S3Exception("BadRequest", "104: Too many buckets", "Bucket", null, HttpResponseStatus.BAD_REQUEST, true);
        } else if (e == S3CODE.S105) {
            return new S3Exception("BadRequest", "105: Invalid bucket name", "Bucket", null, HttpResponseStatus.BAD_REQUEST, true);
        } else if (e == S3CODE.S200) {
            return new S3Exception("NotFound", "200: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S201) {
            return new S3Exception("AccessDenied", "201: Bucket write deny", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S202) {
            return new S3Exception("AccessDenied", "202: Could not get Walrus URL", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S203) {
            return new S3Exception("Conflict", "203: Bucket not empty", "Bucket", null, HttpResponseStatus.CONFLICT, true);
        } else if (e == S3CODE.S300) {
            return new S3Exception("NotFound", "300: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S301) {
            return new S3Exception("AccessDenied", "301: No such user", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S302) {
            return new S3Exception("AccessDenied", "302: Bucket read deny", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S400) {
            return new S3Exception("NotFound", "400: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S401) {
            return new S3Exception("AccessDenied", "401: Get channel error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S402) {
            return new S3Exception("AccessDenied", "402: Get syncid error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S403) {
            return new S3Exception("AccessDenied", "403: Get mtime error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S404) {
            return new S3Exception("AccessDenied", "404: Bucket write deny", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S405) {
            return new S3Exception("AccessDenied", "405: Version is not supported yet", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S406) {
            return new S3Exception("AccessDenied", "406: Object write deny", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S407) {
            return new S3Exception("AccessDenied", "407: CSS error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S500) {
            return new S3Exception("NotFound", "500: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S501) {
            return new S3Exception("NotFound", "501: No such entity", "Entity", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S502) {
            return new S3Exception("AccessDenied", "502: Object read deny", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S503) {
            return new S3Exception("AccessDenied", "503: Object global read deny (torrent)", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S504) {
            return new S3Exception("AccessDenied", "504: Torrent disabled", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S505) {
            return new S3Exception("AccessDenied", "505: Could not create torrent file", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S506) {
            return new S3Exception("AccessDenied", "506: Could not get torrent file", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S507) {
            return new S3Exception("AccessDenied", "507: CSS error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S508) {
            return new S3Exception("BadRequest", "508: Inline data too large", "Entity", null, HttpResponseStatus.BAD_REQUEST, true);
        } else if (e == S3CODE.S600) {
            return new S3Exception("NotFound", "600: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S601) {
            return new S3Exception("AccessDenied", "601: Get syncid error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S602) {
            return new S3Exception("AccessDenied", "602: Bucket write deny", "Bucket", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S603) {
            return new S3Exception("AccessDenied", "603: Version is not supported yet", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S604) {
            return new S3Exception("AccessDenied", "604: SQL error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S605) {
            return new S3Exception("AccessDenied", "605: Write rename event error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S606) {
            return new S3Exception("AccessDenied", "606: CSS error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S700) {
            return new S3Exception("NotFound", "700: No such bucket", "Bucket", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S701) {
            return new S3Exception("NotFound", "701: No such entity", "Entity", null, HttpResponseStatus.NOT_FOUND, true);
        } else if (e == S3CODE.S702) {
            return new S3Exception("AccessDenied", "702: Get syncid error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S703) {
            return new S3Exception("AccessDenied", "703: Object write deny", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S704) {
            return new S3Exception("AccessDenied", "704: CSS error", "Entity", null, HttpResponseStatus.FORBIDDEN, true);
        } else if (e == S3CODE.S999) {
            return new S3Exception("GatewayTimeout", "999: 504 Gateway Time-out", "Entity", null, HttpResponseStatus.GATEWAY_TIMEOUT, true);
        }
        return null;
    }//toS3Exception

    public static S3Exception check(S3METHOD m) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));
            String line;
            while ((line = br.readLine()) != null) {
                int mid = line.indexOf('=');
                int last = line.length();
                String key = line.substring(0, mid);
                String value = line.substring(mid+1, last);
                if (m == getMethod(key)) {
                    S3CODE c = getCode(value);
                    if (c != null) {
                        return toS3Exception(c);
                    }
                    break;
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: FakeResponse.check error : " + t);
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }
        return null;
    }//check
}
