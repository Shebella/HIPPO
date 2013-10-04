/*******************************************************************************
 *Copyright (c) 2009 Eucalyptus Systems, Inc.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, only version 3 of the License.
 * 
 * 
 * This file is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * Please contact Eucalyptus Systems, Inc., 130 Castilian
 * Dr., Goleta, CA 93101 USA or visit <http://www.eucalyptus.com/licenses/>
 * if you need additional information or have any questions.
 * 
 * This file may incorporate work covered under the following copyright and
 * permission notice:
 * 
 * Software License Agreement (BSD License)
 * 
 * Copyright (c) 2008, Regents of the University of California
 * All rights reserved.
 * 
 * Redistribution and use of this software in source and binary forms, with
 * or without modification, are permitted provided that the following
 * conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. USERS OF
 * THIS SOFTWARE ACKNOWLEDGE THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE
 * LICENSED MATERIAL, COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS
 * SOFTWARE, AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
 * IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA, SANTA
 * BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY, WHICH IN
 * THE REGENTS DISCRETION MAY INCLUDE, WITHOUT LIMITATION, REPLACEMENT
 * OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO IDENTIFIED, OR
 * WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT NEEDED TO COMPLY WITH
 * ANY SUCH LICENSES OR RIGHTS.
 *******************************************************************************/
/*
 * Author: chris grzegorczyk <grze@eucalyptus.com>
 */
package com.eucalyptus.ws.handlers;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.eucalyptus.util.WalrusProperties;
import com.eucalyptus.ws.util.*;
import edu.ucsb.eucalyptus.cloud.AccessDeniedException;
import edu.ucsb.eucalyptus.cloud.WalrusException;
import edu.ucsb.eucalyptus.msgs.*;
import edu.ucsb.eucalyptus.util.SbxDB;
import com.eucalyptus.context.SbxRequest;
import edu.ucsb.eucalyptus.util.SbxJobQueue;
import edu.ucsb.eucalyptus.util.SbxRequestPool;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.mule.DefaultMuleEvent;
import org.mule.DefaultMuleMessage;
import org.mule.DefaultMuleSession;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.MuleSession;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.transport.DispatchException;
import org.mule.transport.AbstractConnector;
import org.mule.transport.NullPayload;
import org.mule.transport.vm.VMMessageDispatcherFactory;
import com.eucalyptus.auth.principal.User;
import com.eucalyptus.bootstrap.Component;
import com.eucalyptus.context.Context;
import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.NoSuchContextException;
import com.eucalyptus.context.ServiceContext;
import com.eucalyptus.http.MappingHttpMessage;
import com.eucalyptus.http.MappingHttpRequest;
import com.eucalyptus.http.MappingHttpResponse;
import com.eucalyptus.records.EventClass;
import com.eucalyptus.records.EventRecord;
import com.eucalyptus.records.EventType;
import com.eucalyptus.util.LogUtil;
import com.eucalyptus.ws.client.NioMessageReceiver;
import edu.ucsb.eucalyptus.constants.IsData;

@ChannelPipelineCoverage( "one" )
public class ServiceSinkHandler extends SimpleChannelHandler {
  private static VMMessageDispatcherFactory dispatcherFactory = new VMMessageDispatcherFactory( );
  private static Logger                     LOG               = Logger.getLogger( ServiceSinkHandler.class );
  private AtomicLong                        startTime         = new AtomicLong( 0l );
  
  private NioMessageReceiver                msgReceiver;

  public ServiceSinkHandler( ) {}
  
  public ServiceSinkHandler( final NioMessageReceiver msgReceiver ) {
    this.msgReceiver = msgReceiver;
  }
  
  @Override
  public void exceptionCaught( final ChannelHandlerContext ctx, final ExceptionEvent e ) {//FIXME: handle exceptions cleanly.
    LOG.trace( ctx.getChannel( ), e.getCause( ) );
    Channels.fireExceptionCaught( ctx.getChannel( ), e.getCause( ) );
  }
  
  @SuppressWarnings( "unchecked" )
  @Override
  public void handleDownstream( final ChannelHandlerContext ctx, ChannelEvent e ) throws Exception {
    LOG.trace( this.getClass( ).getSimpleName( ) + "[outgoing]: " + e.getClass( ) );
    if ( e instanceof MessageEvent ) {
      final MessageEvent msge = ( MessageEvent ) e;
      if ( msge.getMessage( ) instanceof NullPayload ) {
        msge.getFuture( ).cancel( );
      } else if ( msge.getMessage( ) instanceof HttpResponse ) {
        ctx.sendDownstream( e );
      } else if ( msge.getMessage( ) instanceof IsData ) {// Pass through for chunked messaging
        ctx.sendDownstream( e );
      } else if ( msge.getMessage( ) instanceof BaseMessage ) {// Handle single request-response MEP
        BaseMessage reply = ( BaseMessage ) ( ( MessageEvent ) e ).getMessage( );
        if ( reply instanceof WalrusDataGetResponseType
             && !( reply instanceof GetObjectResponseType && ( ( GetObjectResponseType ) reply ).getBase64Data( ) != null ) ) {
          e.getFuture( ).cancel( );
          return;
        } else {
          this.sendDownstreamNewEvent( ctx, e, reply );
        }
      } else {
        e.getFuture( ).cancel( );
        LOG.warn( "Non-specific type being written to the channel. Not dropping this message causes breakage:" + msge.getMessage( ).getClass( ) );
      }
      if ( e.getFuture( ).isCancelled( ) ) {
        LOG.trace( "Cancelling send on : " + LogUtil.dumpObject( e ) );
      }
    } else {
      ctx.sendDownstream( e );
    }
  }
  
  private void sendDownstreamNewEvent( ChannelHandlerContext ctx, ChannelEvent e, BaseMessage reply ) {
    MappingHttpRequest request = null;
    Context reqCtx = null;
    String corrid = null;
    try {
      if ( reply != null ) {
        corrid = reply.getCorrelationId();
        reqCtx = Contexts.lookup( corrid );
        request = reqCtx.getHttpRequest( );
      }
    } catch ( NoSuchContextException e1 ) {
      LOG.debug( e1, e1 );
    }
    if ( request != null ) {
      if ( reply == null ) {
        LOG.warn( "Received a null response for request: " + request.getMessageString( ) );
        reply = new EucalyptusErrorMessageType( this.getClass( ).getSimpleName( ), ( BaseMessage ) request.getMessage( ), "Received a NULL reply" );
      }
      //EventRecord.here( reply.getClass( ), EventClass.MESSAGE, EventType.MSG_SERVICED, Long.toString( System.currentTimeMillis( ) - this.startTime.get( ) ) ).trace();
      final MappingHttpResponse response = new MappingHttpResponse( request.getProtocolVersion( ) );
      if (reply.getMetaTooManyRequest() != null) {
          response.addHeader(WalrusProperties.AMZ_META_HEADER_PREFIX + "toomanyrequest", reply.getMetaTooManyRequest());
      }
      final DownstreamMessageEvent newEvent = new DownstreamMessageEvent( ctx.getChannel( ), e.getFuture( ), response, null );
      response.setMessage( reply );
      ctx.sendDownstream( newEvent );
//      Contexts.clear( reqCtx );
        Contexts.sbxclear(corrid);
    }
  }
  
  @Override
  public void handleUpstream( final ChannelHandlerContext ctx, final ChannelEvent e ) throws Exception {
    LOG.trace( this.getClass( ).getSimpleName( ) + "[incoming]: " + e );
    if ( e instanceof ExceptionEvent ) {
      this.exceptionCaught( ctx, ( ExceptionEvent ) e );
    } else if ( e instanceof MessageEvent ) {
      this.startTime.compareAndSet( 0l, System.currentTimeMillis( ) );
      final MessageEvent event = ( MessageEvent ) e;
      if ( event.getMessage( ) instanceof MappingHttpMessage ) {
        final MappingHttpMessage request = ( MappingHttpMessage ) event.getMessage( );
        final User user = Contexts.lookup( request.getCorrelationId( ) ).getUser( );
        final BaseMessage msg = ( BaseMessage ) request.getMessage( );
        final String userAgent = request.getHeader( HttpHeaders.Names.USER_AGENT );
        if ( msg.getCorrelationId( ) == null ) {
          String corrId = null;
          try {
            corrId = Contexts.lookup( ctx.getChannel( ) ).getCorrelationId( );
          } catch ( Exception e1 ) {
            corrId = UUID.randomUUID( ).toString( );
          }
          msg.setCorrelationId( corrId );
        }
        if ( ( userAgent != null ) && userAgent.matches( ".*EucalyptusAdminAccess" ) && msg.getClass( ).getSimpleName( ).startsWith( "Describe" ) ) {
          msg.setEffectiveUserId( msg.getUserId( ) );
        } else if ( ( user != null ) && ( this.msgReceiver == null ) ) {
          msg.setUserId( user.getName( ) );
          msg.setEffectiveUserId( user.isAdministrator( ) ? Component.eucalyptus.name( ) : user.getName( ) );
        }
        //EventRecord.here( ServiceSinkHandler.class, EventType.MSG_RECEIVED, msg.getClass( ).getSimpleName( ) ).trace( );
        if ( this.msgReceiver == null ) {
          ServiceSinkHandler.dispatchRequest(ctx,  msg );
        } else if ( ( user == null ) || ( ( user != null ) && user.isAdministrator( ) ) ) {
          this.dispatchRequest( ctx, request, msg );
        } else {
          //Contexts.clear( Contexts.lookup( msg.getCorrelationId( ) ) );
          ctx.getChannel( ).write( new MappingHttpResponse( request.getProtocolVersion( ), HttpResponseStatus.FORBIDDEN ) );
        }
      } else if ( e instanceof IdleStateEvent ) {
        LOG.warn( "Closing idle connection: " + e );
        e.getFuture( ).addListener( ChannelFutureListener.CLOSE );
        ctx.sendUpstream( e );
      }
      
    }
  }

    private void dispatchRequest( final ChannelHandlerContext ctx, final MappingHttpMessage request, final BaseMessage msg ) throws NoSuchContextException {
    try {
      final MuleMessage reply = this.msgReceiver.routeMessage( new DefaultMuleMessage( msg ), true );
      if ( reply != null ) {
        ReplyQueue.handle( this.msgReceiver.getService( ).getName( ), reply, msg );
      } else {
        //EventRecord.here( ServiceSinkHandler.class, EventType.MSG_SENT_ASYNC, msg.getClass( ).getSimpleName( ), this.msgReceiver.getEndpointURI( ).toString( ) );
      }
    } catch ( Exception e1 ) {
      LOG.error( e1, e1 );
      EucalyptusErrorMessageType errMsg = new EucalyptusErrorMessageType( this.msgReceiver.getService( ).getName( ), msg,
                                                                          ( e1.getCause( ) != null ? e1.getCause( ) : e1 ).getMessage( ) );
      errMsg.setCorrelationId( msg.getCorrelationId( ) );
      errMsg.setException( e1.getCause( ) != null ? e1.getCause( ) : e1 );
      //Contexts.clear( Contexts.lookup( errMsg.getCorrelationId( ) ) );
      Channels.write( ctx.getChannel( ), errMsg );
    }
  }


    public static void handle(BaseMessage responseMessage, SbxRequest req) {
        String status = "200 (OK)";
        String message = null;
        String resource = null;
        String type = (req != null ? req.type : "unknown");
        String corrid = null;
        Channel channel = null;
        boolean bWrite = false;
        String objkey = null;
        try {
            SbxRate.inc();
            if (req == null) {
                status = "handle";
                message = "req is null";
                SbxLog.log(req, type, status, message, resource);
                SbxJobLog.log(req, status, message);
                return;
            }
            if (req.put || req.rename) {
                try {
                    if (req.msg != null && req.msg instanceof PutObjectType) {
                        if (req.rename) {
                            objkey = ((PutObjectType) req.msg).getKey() + "->" + ((PutObjectType) req.msg).getRenameto();
                        } else {
                            objkey = ((PutObjectType) req.msg).getKey();
                        }
                    }
                } catch (Throwable e) {
                    //NOP
                }
            } else if (req.get || req.getdetail) {
                try {
                    if (req.msg != null && req.msg instanceof GetObjectType) {
                        objkey = ((GetObjectType) req.msg).getKey();
                    }
                } catch (Throwable e) {
                    //NOP
                }
            }
            if (responseMessage != null) {
                corrid = responseMessage.getCorrelationId( );
                Context context = Contexts.lookup(corrid);
                channel = context.getChannel( );
                bWrite = true;
                Channels.write(channel, responseMessage);
            }
            resource = objkey;
            SbxLog.log(req, type, status, message, resource);
            SbxJobLog.log(req);
        } catch ( Throwable t ) {
            try {
                if (channel != null && responseMessage != null && bWrite == false) {
                    Channels.write(channel, responseMessage);
                }
            } catch (Throwable e) {
                //NOP
            }
            status = "handle";
            message = "Throwable";
            resource = objkey + " | " + t.toString();
            SbxLog.log(req, type, status, message, resource);
            SbxJobLog.log(req);
        }
    }

    public static void handleException(Throwable t, SbxRequest req){
        String status = null;
        String message = null;
        String resource = null;
        String type = (req != null ? req.type : "unknown");
        String corrid = null;
        Channel channel = null;
        EucalyptusErrorMessageType errMsg = null;
        boolean bWrite = false;
        String objkey = null;
        try {
            SbxRate.inc();
            if (req == null) {
                status = "handleException";
                message = "req is null";
                SbxLog.log(req, null, status, message, resource);
                SbxJobLog.log(req, status, message);
                return;
            }
            if (req.put || req.rename) {
                try {
                    if (req.msg != null && req.msg instanceof PutObjectType) {
                        if (req.rename) {
                            objkey = ((PutObjectType) req.msg).getKey() + "->" + ((PutObjectType) req.msg).getRenameto();
                        } else {
                            objkey = ((PutObjectType) req.msg).getKey();
                        }
                    }
                } catch (Throwable e) {
                    //NOP
                }
            } else if (req.get || req.getdetail) {
                try {
                    if (req.msg != null && req.msg instanceof GetObjectType) {
                        objkey = ((GetObjectType) req.msg).getKey();
                    }
                } catch (Throwable e) {
                    //NOP
                }
            }
            corrid = Contexts.getCorrId(req);
            channel = Contexts.getChannel(req);
            if (req.msg != null) {
                errMsg = new EucalyptusErrorMessageType(type, req.msg,( t.getCause( ) != null ? t.getCause( ) : t ).getMessage( ) );
            }
            if (channel != null && errMsg != null) {
                errMsg.setCorrelationId(corrid);
                errMsg.setException( t.getCause( ) != null ? t.getCause( ) : t );
                bWrite = true;
                Channels.write(channel, errMsg);
                if (t instanceof WalrusException) {
                    WalrusException we = (WalrusException) t;
                    status = we.getCode() + "(" + we.getStatus() + ")";
                    message = we.getMessage();
                    resource = we.getResource() + " | " + objkey;
                } else {
                    status = "handleException";
                    message = "NotWalrusException";
                    resource = objkey + " | " + t.toString();
                }
                SbxLog.log(req, type, status, message, resource);
                SbxJobLog.log(req, status, message);
            } else {
                status = "handleException(" + req.msg + ")";
                message = corrid;
                resource = (channel != null ? channel.toString() : null) + " | " + objkey;
                SbxLog.log(req, req.type, status, message, resource);
                SbxJobLog.log(req, status, message);
            }
        } catch(Throwable tt){
            try {
                if (channel != null && errMsg != null && bWrite == false) {
                    Channels.write(channel, errMsg);
                }
            } catch (Throwable e) {
                //NOP
            }
            status = "handleException";
            message = "Throwable";
            resource = objkey + " | " + tt.toString();
            SbxLog.log(req, type, status, message, resource);
            SbxJobLog.log(req, status, message);
        }
    }


    public static void ServerTooBusy(SbxRequest r) {
        ServerTooBusy(r, false);
    }

    public static void ServerTooBusy(SbxRequest r, boolean isTimeOut) {
        String status = "AccessDenied (403)";
        String message = isTimeOut == true ? "TooManyRequests(TimeOut)" : "TooManyRequests";
        String resource = null;
        String type = (r != null ? r.type : "unknown");
        String corrid = null;
        Channel channel = null;
        EucalyptusErrorMessageType errMsg = null;
        boolean bWrite = false;
        String objkey = null;
        try {
            SbxRate.inc();
            if (r == null) {
                status = "ServerTooBusy";
                String msg = "req is null(" + message + ")";
                SbxLog.log(r, null, status, msg, resource);
                SbxJobLog.log(r, status, msg);
                return;
            }
            if (r.put || r.rename) {
                try {
                    if (r.msg != null && r.msg instanceof PutObjectType) {
                        if (r.rename) {
                            objkey = ((PutObjectType) r.msg).getKey() + "->" + ((PutObjectType) r.msg).getRenameto();
                        } else {
                            objkey = ((PutObjectType) r.msg).getKey();
                        }
                    }
                } catch (Throwable t) {
                    //NOP
                }
            } else if (r.get || r.getdetail) {
                try {
                    if (r.msg != null && r.msg instanceof GetObjectType) {
                        objkey = ((GetObjectType) r.msg).getKey();
                    }
                } catch (Throwable t) {
                    //NOP
                }
            }
            corrid = Contexts.getCorrId(r);
            channel = Contexts.getChannel(r);
            Object rate = SbxRate.getRate();
            AccessDeniedException e = new AccessDeniedException(message, (String)rate, true);
            if (r.msg != null) {
                errMsg = new EucalyptusErrorMessageType(message, r.msg, (e.getCause() != null ? e.getCause() : e).getMessage());
            }
            if (channel != null && errMsg != null) {
                errMsg.setCorrelationId(corrid);
                errMsg.setMetaTooManyRequest((String) rate);
                errMsg.setException(e.getCause() != null ? e.getCause() : e);
                bWrite = true;
                Channels.write(channel, errMsg);
                resource = (String) rate + " | " + objkey;
                SbxLog.log(r, type, status, message, resource);
                SbxJobLog.log(r, status, message);
            } else {
                status = "ServerTooBusy(" + r.msg + ")";
                resource = corrid + "|" + (channel != null ? channel.toString() : null) + " | " + objkey;
                SbxLog.log(r, r.type, status, message, resource);
                SbxJobLog.log(r, status, message);
            }
        } catch(Throwable t) {
            try {
                if (channel != null && errMsg != null && bWrite == false) {
                    Channels.write(channel, errMsg);
                }
            } catch (Throwable e) {
                //NOP
            }
            status = "ServerTooBusy";
            String msg = "Throwable(" + message + ")";
            resource = objkey + " | " + t.toString();
            SbxLog.log(r, type, status, msg, resource);
            SbxJobLog.log(r, status, msg);
        }
    }

    public static void ServerResult(SbxRequest r, String message) {
        String status = "AccessDenied (403)";
        String resource = null;
        String type = (r != null ? r.type : "unknown");
        String corrid = null;
        Channel channel = null;
        EucalyptusErrorMessageType errMsg = null;
        boolean bWrite = false;
        String objkey = null;
        try {
            SbxRate.inc();
            if (r == null) {
                status = "ServerResult";
                resource = message;
                message = "req is null";
                SbxLog.log(r, null, status, message, resource);
                SbxJobLog.log(r, status, message);
                return;
            }
            if (r.put || r.rename) {
                try {
                    if (r.msg != null && r.msg instanceof PutObjectType) {
                        if (r.rename) {
                            objkey = ((PutObjectType) r.msg).getKey() + "->" + ((PutObjectType) r.msg).getRenameto();
                        } else {
                            objkey = ((PutObjectType) r.msg).getKey();
                        }
                    }
                } catch (Throwable t) {
                    //NOP
                }
            } else if (r.get || r.getdetail) {
                try {
                    if (r.msg != null && r.msg instanceof GetObjectType) {
                        objkey = ((GetObjectType) r.msg).getKey();
                    }
                } catch (Throwable t) {
                    //NOP
                }
            }
            corrid = Contexts.getCorrId(r);
            channel = Contexts.getChannel(r);
            AccessDeniedException e = new AccessDeniedException(message, "", true);
            if (r.msg != null) {
                errMsg = new EucalyptusErrorMessageType(message, r.msg, (e.getCause() != null ? e.getCause() : e).getMessage());
            }
            if (channel != null && errMsg != null) {
                errMsg.setCorrelationId(corrid);
                errMsg.setException(e.getCause() != null ? e.getCause() : e);
                bWrite = true;
                Channels.write(channel, errMsg);
                resource = objkey;
                SbxLog.log(r, type, status, message, resource);
                SbxJobLog.log(r, status, message);
            } else {
                status = "ServerResult(" + r.msg + ")";
                resource = corrid + "|" + (channel != null ? channel.toString() : null) + " | " + objkey;
                SbxLog.log(r, type, status, message, resource);
                SbxJobLog.log(r, status, message);
            }
        } catch(Throwable t) {
            try {
                if (channel != null && errMsg != null && bWrite == false) {
                    Channels.write(channel, errMsg);
                }
            } catch (Throwable e) {
                //NOP
            }
            status = "ServerResult";
            message = "Throwable";
            resource = objkey + " | " + t.toString();
            SbxLog.log(r, type, status, message, resource);
            SbxJobLog.log(r, status, message);
        }
    }

    private static int VIP_NUMBER = System.getProperty("euca.sbxrequest.vipthread")==null ? 40 : Integer.valueOf(System.getProperty("euca.sbxrequest.vipthread"));
    private static Executor ec= Executors.newFixedThreadPool(VIP_NUMBER); // VIP operations
    private static void dispatchRequest( final ChannelHandlerContext ctx,  final BaseMessage msg ) throws MuleException, DispatchException {
        SbxRequest req = null;
        String retry = null;
        String hold = null;
        try {
            if(msg instanceof PutObjectType){
                req = new SbxRequest();
                if(((PutObjectType)msg).getRenameto()!=null){
                    req.type = "RenameObject";
                    req.rename = true;
                } else {
                    req.type = "PutObject";
                    req.put = true;
                    try {
                        req.content_length = Long.valueOf(((PutObjectType)msg).getContentLength());
                    } catch (Throwable t) {
                        //NOP
                    }
                }
            }else if(msg instanceof GetObjectType){
                req = new SbxRequest();
                if (((GetObjectType)msg).getGetData()) {
                    req.type = "GetObject";
                    req.get = true;
                } else {
                    req.type = "GetObjectDetails";
                    req.getdetail = true;
                }
            }else if(msg instanceof DeleteObjectType){
                req = new SbxRequest();
                req.type = "DeleteObject";
            }else if(msg instanceof ListAllMyBucketsType){
                req = new SbxRequest();
                req.type = "ListAllMyBuckets";
            }else if(msg instanceof CreateBucketType){
                req = new SbxRequest();
                req.type = "CreateBucket";
            }else if(msg instanceof DeleteBucketType){
                req = new SbxRequest();
                req.type = "DeleteBucket";
            }else if(msg instanceof ListBucketType){
                req = new SbxRequest();
                req.type = "ListBucket";
            }
            if (req != null) {
                req.msg = msg;
                req.channelctx = ctx;
                req.userctx = Contexts.lookup(msg.getCorrelationId());
                req.reqId = msg.getMetaReqId();
                retry = msg.getMetaRetry();
                hold = msg.getMetaHold();

                if (Contexts.SBXSVR) {
                    //retry mechanism
                    //Contexts.add(req);
                    boolean bRetry = false;
                    if (retry != null && retry.equals("retry")) bRetry = true;
                    if (bRetry) {
                        try {
                            //lookup opt_log
                            String username = req.userctx.getUser().getName();
                            if (SbxDB.lookup_opt_log(username, req.reqId)) {
                                ServerResult(req, "ServerSuccess");
                                return;
                            }
                            //work around : no ServerInProgress for PUT
                            if (req.put == false) {
                                //lookup Contexts
                                boolean re = Contexts.find1st(req);
                                if (re) {
                                    //1st request is in-progress
                                    ServerResult(req, "ServerInProgress");
                                    return;
                                }
                            }
                        } catch (Throwable t) {
                            LOG.debug("THROTTLINGandRETRY: dispatchRequest.error : retry error : " + req + "###" + t);
                        }
                    }
                    //VIP operations
                    if (hold != null && hold.equals("VIP")) {
                        Object vip = Class.forName("edu.ucsb.eucalyptus.cloud.ws.VipOperation").newInstance();
                        Method mth = vip.getClass().getMethod("init", boolean.class, SbxRequest.class);
                        mth.invoke(vip, true, req);
                        ec.execute((Runnable) vip);
                        return;
                    }
                    //too many put requests
                    if (req.put && SbxPutJobQueue.toomanyrequests()) {
                        ServerTooBusy(req);
                        return;
                    }
                    //too many get requests
                    if (req.get && SbxGetJobQueue.toomanyrequests()) {
                        ServerTooBusy(req);
                        return;
                    }
                    //insert into user request pool
                    if (SbxRequestPool.offer(msg.getUserId(), req) == null) {
                        //user request queue is full
                        ServerTooBusy(req);
                        return;
                    }
                } else {
                    if (req.put) {
                        SbxPutRequest r = new SbxPutRequest(req);
                        if (SbxPutJobQueue.offer(r) == null) {
                            ServerTooBusy(req);
                        }
                        return;
                    }
                    if (req.get) {
                        if (SbxGetJobQueue.offer(req) == null) {
                            ServerTooBusy(req);
                        }
                        return;
                    }
                    if (SbxJobQueue.offer(req) == null) {
                        ServerTooBusy(req);
                        return;
                    }
                }
                return;
            }
        }catch (Throwable t) {
            SbxLog.log(req, req.type, "MULE", "dispatch request error: " + t.toString(), "using mule");
        }

        OutboundEndpoint endpoint = ServiceContext.getContext( ).getRegistry( ).lookupEndpointFactory( ).getOutboundEndpoint( "vm://RequestQueue" );
        if ( !endpoint.getConnector( ).isStarted( ) ) {
          endpoint.getConnector( ).start( );
        }
        MuleMessage muleMsg = new DefaultMuleMessage( msg );
        MuleSession muleSession = new DefaultMuleSession( muleMsg, ( ( AbstractConnector ) endpoint.getConnector( ) ).getSessionHandler( ),
                                                          ServiceContext.getContext( ) );
        MuleEvent muleEvent = new DefaultMuleEvent( muleMsg, endpoint, muleSession, false );
        dispatcherFactory.create( endpoint ).dispatch( muleEvent );
    }
  
  
  
  /*
  private static void dispatchRequest( final BaseMessage msg ) throws MuleException, DispatchException {
    OutboundEndpoint endpoint = ServiceContext.getContext( ).getRegistry( ).lookupEndpointFactory( ).getOutboundEndpoint( "vm://RequestQueue" );
    if ( !endpoint.getConnector( ).isStarted( ) ) {
      endpoint.getConnector( ).start( );
    }
    MuleMessage muleMsg = new DefaultMuleMessage( msg );
    MuleSession muleSession = new DefaultMuleSession( muleMsg, ( ( AbstractConnector ) endpoint.getConnector( ) ).getSessionHandler( ),
                                                      ServiceContext.getContext( ) );
    MuleEvent muleEvent = new DefaultMuleEvent( muleMsg, endpoint, muleSession, false );
    dispatcherFactory.create( endpoint ).dispatch( muleEvent );
  }
  */
  @Override
  public void channelClosed( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception {
//    try {
//      Contexts.clear( Contexts.lookup( ctx.getChannel( ) ) );
//    } catch ( Throwable e1 ) {
//      LOG.warn( "Failed to remove the channel context on connection close.", e1 );
//    }
    super.channelClosed( ctx, e );
  }
  
  @Override
  public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {
    super.messageReceived( ctx, e );
  }
  
}
