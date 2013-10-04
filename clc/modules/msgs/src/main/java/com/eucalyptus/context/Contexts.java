package com.eucalyptus.context;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.mule.RequestContext;
import org.mule.api.MuleMessage;
import com.eucalyptus.http.MappingHttpRequest;
import edu.ucsb.eucalyptus.msgs.BaseMessage;

public class Contexts {
  private static Logger LOG = Logger.getLogger( Contexts.class );
  public static int                             MAX             = 8192;
  public static int                             CONCUR          = MAX / ( Runtime.getRuntime( ).availableProcessors( ) * 2 + 1 );
  public static float                           THRESHOLD       = 1.0f;
  public static ConcurrentMap<String, Context>  uuidContexts    = new ConcurrentHashMap<String, Context>( MAX, THRESHOLD, CONCUR );
  public static ConcurrentMap<Channel, Context> channelContexts = new ConcurrentHashMap<Channel, Context>( MAX, THRESHOLD, CONCUR );
  public static ConcurrentMap<String, StringBuffer> reqidContexts = new ConcurrentHashMap<String, StringBuffer>( MAX, THRESHOLD, CONCUR );
  public static boolean SBXSVR = System.getProperty("euca.safebox.enable") == null ? true : Boolean.valueOf(System.getProperty("euca.safebox.enable"));

  public static Context create( MappingHttpRequest request, Channel channel ) {
    Context ctx = new Context( request, channel );
    request.setCorrelationId( ctx.getCorrelationId( ) );
    uuidContexts.put( ctx.getCorrelationId( ), ctx );
    channelContexts.put( channel, ctx );
    return ctx;
  }

    public static void add(SbxRequest req) {
        try {
            if (req == null) return;
            if (req.reqId == null) return;

            String corrid = getCorrId(req);
            if (corrid == null) return;

            StringBuffer s = reqidContexts.get(req.reqId);
            if (s == null) {
                reqidContexts.put(req.reqId, new StringBuffer(corrid));
            } else {
                if (!s.toString().contains(corrid)) {
                    s.append(" " + corrid);
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: Contexts.add error : " + req + "###" + t);
        }
    }

    public static String getCorrId(SbxRequest r) {
        if (r == null) return null;
        String corrid = null;
        try {
            if (r.msg != null){
                corrid = r.msg.getCorrelationId();
            }
            if (r.userctx != null && corrid == null) {
                corrid = r.userctx.getCorrelationId();
            }
        } catch (Throwable t) {
            //NOP
        }
        return corrid;
    }

    public static String getCorrId(Channel channel) {
        if (channel == null) return null;
        String corrid = null;
        try {
            if ( !channelContexts.containsKey( channel ) ) {
                return null;
            } else {
                Context ctx = channelContexts.get( channel );
                if (ctx != null) {
                    corrid = ctx.getCorrelationId();
                }
            }
        } catch (Throwable t) {
            //NOP
        }
        return corrid;
    }

    public static Channel getChannel(SbxRequest r) {
        if (r == null) return null;
        Channel channel = null;
        try {
            if (r.userctx != null) {
                channel = r.userctx.getChannel();
            }
            if (channel == null) {
                String corrid = getCorrId(r);
                if (corrid != null) {
                    Context ctx = uuidContexts.get(corrid);
                    channel = ctx.getChannel();
                }
            }
        } catch (Throwable t) {
            //NOP
        }
        return channel;
    }

    public static boolean bExit(SbxRequest r) {
        boolean bExit = true;
        try {
            String corrid = getCorrId(r);
            Channel channel = getChannel(r);
            if (channel != null && Contexts.channelContexts.containsKey(channel) &&
                    corrid != null && Contexts.uuidContexts.containsKey(corrid)) {
                bExit = false;
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: Contexts.bExit error : " + account + "###" + corrId + "###" + t);
        }
        return bExit;
    }

    public static boolean bPartialExit(String corrid) {
        boolean bExit = true;
        try {
            if (corrid != null && Contexts.uuidContexts.containsKey(corrid)) {
                bExit = false;
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: Contexts.bPartialExit error : " + account + "###" + corrId + "###" + t);
        }
        return bExit;
    }

    public static void cleanup() {
        try {
            Set<String> set = reqidContexts.keySet();
            Iterator<String> iterator = set.iterator();
            while (iterator.hasNext()) {
                String reqid = iterator.next();
                if (reqid != null) {
                    StringBuffer s = reqidContexts.get(reqid);
                    if (s != null) {
                        String[] uuids = s.toString().split(" ");
                        StringBuffer s2 = new StringBuffer(s.length());
                        boolean replace = false;
                        for (int i=0; i<uuids.length; i++) {
                            if (bPartialExit(uuids[i]) == false) {
                                if (s2.length() <= 0) {
                                    s2.append(uuids[i]);
                                } else {
                                    s2.append(" " + uuids[i]);
                                }
                            } else {
                                replace = true;
                            }
                        }
                        if (s2.length() <= 0) {
                            reqidContexts.remove(reqid);
                        } else if (replace) {
                            reqidContexts.replace(reqid, s2);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: Contexts.cleanup(SbxPutJobQueue) error : " + reqId + " : " + corrId + " : " + t);
        }
    }

    public static boolean find1st(SbxRequest req) {
        try {
            if (req == null) return false;
            if (req.reqId == null) return false;
            String corrid = getCorrId(req);
            if (corrid == null) return false;
            StringBuffer s = reqidContexts.get(req.reqId);
            if (s != null) {
                String[] uuids = s.toString().split(" ");
                for (int i=0; i<uuids.length; i++) {
                    if (uuids[i] == null) continue;
                    if (uuids[i].isEmpty()) continue;
                    if (bPartialExit(uuids[i])) continue;
                    if (uuids[i].equals(corrid) == false) {
                        return true;
                    }
                }
            }
        }catch (Throwable t) {
            //LOG.debug("THROTTLINGandRETRY: Contexts.find1st error : " + req + "###" + t);
        }
        return false;
    }



    public static Context lookup( Channel channel ) throws NoSuchContextException {
    if ( !channelContexts.containsKey( channel ) ) {
      throw new NoSuchContextException( "Found channel context " + channel + " but no corresponding context." );
    } else {
      Context ctx = channelContexts.get( channel );
      ctx.setMuleEvent( RequestContext.getEvent( ) );
      return ctx;
    }
  }
  
  public static Context lookup( String correlationId ) throws NoSuchContextException {
    if ( !uuidContexts.containsKey( correlationId ) ) {
      throw new NoSuchContextException( "Found correlation id " + correlationId + " but no corresponding context." );
    } else {
      Context ctx = uuidContexts.get( correlationId );
      ctx.setMuleEvent( RequestContext.getEvent( ) );
      return ctx;
    }
  }

  public static Context lookup( ) {
    BaseMessage parent = null;
    MuleMessage muleMsg = null;
    if ( RequestContext.getEvent( ) != null && RequestContext.getEvent( ).getMessage( ) != null ) {
      muleMsg = RequestContext.getEvent( ).getMessage( );
    } else if ( RequestContext.getEventContext( ) != null && RequestContext.getEventContext( ).getMessage( ) != null ) {
      muleMsg = RequestContext.getEventContext( ).getMessage( );
    } else {
      throw new IllegalContextAccessException( "Cannot access context implicitly using lookup(V) outside of a service." );
    }
    Object o = muleMsg.getPayload( );
    if ( o != null && o instanceof BaseMessage ) {
      String correlationId = ( ( BaseMessage ) o ).getCorrelationId( );
      try {
        return Contexts.lookup( correlationId );
      } catch ( NoSuchContextException e ) {
        //LOG.error( e, e );
        throw new IllegalContextAccessException( "Cannot access context implicitly using lookup(V) when not handling a request.", e );
      }
    } else {
      throw new IllegalContextAccessException( "Cannot access context implicitly using lookup(V) when not handling a request." );
    }
  }

    public static void clear( Context context ) {
        Context ctx = uuidContexts.remove(context.getCorrelationId());
        Channel channel = null;
        if ( ctx != null && ( channel = ctx.getChannel( ) ) != null ) {
            channelContexts.remove( channel );
        } else {
            throw new RuntimeException( "Missing reference to channel for the request." );
        }
        ctx.clear( );
    }

    public static void sbxclear( String corrid ) {
        if (corrid == null) return;
        Context ctx = null;
        try {
            ctx = uuidContexts.remove(corrid);
        } catch (Throwable t) {
            //NOP
        }
        try {
            if (ctx != null) {
                Channel channel = ctx.getChannel();
                if (channel != null) {
                    channelContexts.remove(channel);
                }
            }
        } catch (Throwable t) {
            //NOP
        }
    }

    public static void sbxclear( SbxRequest req ) {
        if (req == null) return;
        try {
            String corrid = getCorrId(req);
            if (corrid != null) {
                uuidContexts.remove(corrid);
            }
        } catch (Throwable t) {
            //NOP
        }
        try {
            Channel channel = getChannel(req);
            if (channel != null) {
                channelContexts.remove(channel);
            }
        } catch (Throwable t) {
            //NOP
        }
    }

}
