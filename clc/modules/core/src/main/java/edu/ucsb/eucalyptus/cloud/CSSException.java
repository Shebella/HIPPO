package edu.ucsb.eucalyptus.cloud;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/4/10
 * Time: 7:58
 * To change this template use File | Settings | File Templates.
 */
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

@SuppressWarnings("serial")
public class CSSException extends WalrusException {

    public CSSException()
    {
        super("CSSException", "CSS Exception", "Entity", "CSS Exception", HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
    public CSSException(String entity)
    {
        super("CSSException", "CSS Exception", "Entity", entity, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    public CSSException(String entityType, String entity)
    {
        super("CSSException", "CSS Exception", entityType, entity, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    public CSSException(String entityType, String entity, BucketLogData logData)
    {
        super("CSSException", "CSS Exception", entityType, entity, HttpResponseStatus.INTERNAL_SERVER_ERROR, logData);
    }

    public CSSException(Throwable ex)
    {
        super("CSS Exception", ex);
    }
    public CSSException(String message, Throwable ex)
    {
        super(message,ex);
    }
}
