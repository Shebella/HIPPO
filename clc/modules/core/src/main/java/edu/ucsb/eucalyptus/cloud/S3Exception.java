package edu.ucsb.eucalyptus.cloud;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  11:11
 * To change this template use File | Settings | File Templates.
 */
public class S3Exception extends WalrusException  {

    public S3Exception(String code, String message, String type, String resource, HttpResponseStatus status, boolean nop)
    {
        super(code, message, type, resource, status);
    }
}