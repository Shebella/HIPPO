package com.eucalyptus.ws.handlers.http;

import java.io.IOException;

import edu.ucsb.eucalyptus.msgs.EucalyptusErrorMessageType;
import edu.ucsb.eucalyptus.msgs.EucalyptusMessage;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.Context;
import com.eucalyptus.http.MappingHttpResponse;

public class NioHttpEncoder extends HttpResponseEncoder{

	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent evt) throws Exception {
	}
	
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception{

		super.handleDownstream(ctx, evt);				
//		try {
//			super.handleDownstream(ctx, evt);				
//			if (ctx.canHandleDownstream()) {
//				Context context = Contexts.lookup( ctx.getChannel() );
//		        Contexts.clear( context );		
//			}
//		} catch (Exception e) {		
//		} finally {	
//		}
	}	
}

