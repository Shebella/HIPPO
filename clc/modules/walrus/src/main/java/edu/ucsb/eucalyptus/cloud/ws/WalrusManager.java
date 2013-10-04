/*******************************************************************************
 *Copyright (c) 2009  Eucalyptus Systems, Inc.
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, only version 3 of the License.
 * 
 * 
 *  This file is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 * 
 *  You should have received a copy of the GNU General Public License along
 *  with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  Please contact Eucalyptus Systems, Inc., 130 Castilian
 *  Dr., Goleta, CA 93101 USA or visit <http://www.eucalyptus.com/licenses/>
 *  if you need additional information or have any questions.
 * 
 *  This file may incorporate work covered under the following copyright and
 *  permission notice:
 * 
 *    Software License Agreement (BSD License)
 * 
 *    Copyright (c) 2008, Regents of the University of California
 *    All rights reserved.
 * 
 *    Redistribution and use of this software in source and binary forms, with
 *    or without modification, are permitted provided that the following
 *    conditions are met:
 * 
 *      Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 * 
 *      Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 * 
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 *    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 *    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 *    OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. USERS OF
 *    THIS SOFTWARE ACKNOWLEDGE THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE
 *    LICENSED MATERIAL, COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS
 *    SOFTWARE, AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
 *    IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA, SANTA
 *    BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY, WHICH IN
 *    THE REGENTS DISCRETION MAY INCLUDE, WITHOUT LIMITATION, REPLACEMENT
 *    OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO IDENTIFIED, OR
 *    WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT NEEDED TO COMPLY WITH
 *    ANY SUCH LICENSES OR RIGHTS.
 *******************************************************************************/
package edu.ucsb.eucalyptus.cloud.ws;

/*
 *
 * Author: Sunil Soman sunils@cs.ucsb.edu
 */

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;

import com.eucalyptus.context.SbxRequest;
import com.eucalyptus.ws.util.SbxPayloadLog;
import com.eucalyptus.ws.util.SbxPutJobQueue;
import com.eucalyptus.ws.util.SbxPutRequest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.ucsb.eucalyptus.cloud.*;

import edu.ucsb.eucalyptus.util.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.tools.ant.util.DateUtils;
import org.hibernate.CacheMode;
import org.hibernate.Hibernate;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.mortbay.log.Log;


import com.eucalyptus.auth.NoSuchUserException;
import com.eucalyptus.auth.Users;
import com.eucalyptus.auth.crypto.Digest;
import com.eucalyptus.auth.principal.User;
import com.eucalyptus.auth.util.Hashes;
import com.eucalyptus.bootstrap.Component;
import com.eucalyptus.context.Contexts;
import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.util.WalrusProperties;
import com.eucalyptus.ws.client.ServiceDispatcher;
import com.eucalyptus.ws.handlers.WalrusRESTBinding;

import edu.ucsb.eucalyptus.cloud.entities.BucketInfo;
import edu.ucsb.eucalyptus.cloud.entities.GrantInfo;
import edu.ucsb.eucalyptus.cloud.entities.ImageCacheInfo;
import edu.ucsb.eucalyptus.cloud.entities.MetaDataInfo;
import edu.ucsb.eucalyptus.cloud.entities.ObjectInfo;
import edu.ucsb.eucalyptus.cloud.entities.SystemConfiguration;
import edu.ucsb.eucalyptus.cloud.entities.TorrentInfo;
import edu.ucsb.eucalyptus.cloud.entities.WalrusInfo;
import edu.ucsb.eucalyptus.cloud.entities.WalrusSnapshotInfo;
import edu.ucsb.eucalyptus.msgs.AccessControlListType;
import edu.ucsb.eucalyptus.msgs.AccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.AddObjectResponseType;
import edu.ucsb.eucalyptus.msgs.AddObjectType;
import edu.ucsb.eucalyptus.msgs.BucketListEntry;
import edu.ucsb.eucalyptus.msgs.CanonicalUserType;
import edu.ucsb.eucalyptus.msgs.CopyObjectResponseType;
import edu.ucsb.eucalyptus.msgs.CopyObjectType;
import edu.ucsb.eucalyptus.msgs.CreateBucketResponseType;
import edu.ucsb.eucalyptus.msgs.CreateBucketType;
import edu.ucsb.eucalyptus.msgs.DeleteBucketResponseType;
import edu.ucsb.eucalyptus.msgs.DeleteBucketType;
import edu.ucsb.eucalyptus.msgs.DeleteMarkerEntry;
import edu.ucsb.eucalyptus.msgs.DeleteObjectResponseType;
import edu.ucsb.eucalyptus.msgs.DeleteObjectType;
import edu.ucsb.eucalyptus.msgs.DeleteVersionResponseType;
import edu.ucsb.eucalyptus.msgs.DeleteVersionType;
import edu.ucsb.eucalyptus.msgs.GetBucketAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.GetBucketAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.GetBucketLocationResponseType;
import edu.ucsb.eucalyptus.msgs.GetBucketLocationType;
import edu.ucsb.eucalyptus.msgs.GetBucketLoggingStatusResponseType;
import edu.ucsb.eucalyptus.msgs.GetBucketLoggingStatusType;
import edu.ucsb.eucalyptus.msgs.GetBucketVersioningStatusResponseType;
import edu.ucsb.eucalyptus.msgs.GetBucketVersioningStatusType;
import edu.ucsb.eucalyptus.msgs.GetObjectAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.GetObjectAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.GetObjectExtendedResponseType;
import edu.ucsb.eucalyptus.msgs.GetObjectExtendedType;
import edu.ucsb.eucalyptus.msgs.GetObjectResponseType;
import edu.ucsb.eucalyptus.msgs.GetObjectType;
import edu.ucsb.eucalyptus.msgs.Grant;
import edu.ucsb.eucalyptus.msgs.Grantee;
import edu.ucsb.eucalyptus.msgs.Group;
import edu.ucsb.eucalyptus.msgs.ListAllMyBucketsList;
import edu.ucsb.eucalyptus.msgs.ListAllMyBucketsResponseType;
import edu.ucsb.eucalyptus.msgs.ListAllMyBucketsType;
import edu.ucsb.eucalyptus.msgs.ListBucketResponseType;
import edu.ucsb.eucalyptus.msgs.ListBucketType;
import edu.ucsb.eucalyptus.msgs.ListEntry;
import edu.ucsb.eucalyptus.msgs.ListVersionsResponseType;
import edu.ucsb.eucalyptus.msgs.ListVersionsType;
import edu.ucsb.eucalyptus.msgs.LoggingEnabled;
import edu.ucsb.eucalyptus.msgs.MetaDataEntry;
import edu.ucsb.eucalyptus.msgs.PostObjectResponseType;
import edu.ucsb.eucalyptus.msgs.PostObjectType;
import edu.ucsb.eucalyptus.msgs.PrefixEntry;
import edu.ucsb.eucalyptus.msgs.PutObjectInlineResponseType;
import edu.ucsb.eucalyptus.msgs.PutObjectInlineType;
import edu.ucsb.eucalyptus.msgs.PutObjectResponseType;
import edu.ucsb.eucalyptus.msgs.PutObjectType;
import edu.ucsb.eucalyptus.msgs.RemoveARecordType;
import edu.ucsb.eucalyptus.msgs.SetBucketAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.SetBucketAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.SetBucketLoggingStatusResponseType;
import edu.ucsb.eucalyptus.msgs.SetBucketLoggingStatusType;
import edu.ucsb.eucalyptus.msgs.SetBucketVersioningStatusResponseType;
import edu.ucsb.eucalyptus.msgs.SetBucketVersioningStatusType;
import edu.ucsb.eucalyptus.msgs.SetObjectAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.SetObjectAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.SetRESTBucketAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.SetRESTBucketAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.SetRESTObjectAccessControlPolicyResponseType;
import edu.ucsb.eucalyptus.msgs.SetRESTObjectAccessControlPolicyType;
import edu.ucsb.eucalyptus.msgs.Status;
import edu.ucsb.eucalyptus.msgs.TargetGrants;
import edu.ucsb.eucalyptus.msgs.UpdateARecordType;
import edu.ucsb.eucalyptus.msgs.VersionEntry;
import edu.ucsb.eucalyptus.storage.StorageManager;
import edu.ucsb.eucalyptus.storage.fs.FileIO;

public class WalrusManager {
	private static Logger LOG = Logger.getLogger(WalrusManager.class);

	private StorageManager storageManager;
	private WalrusImageManager walrusImageManager;
	private static WalrusStatistics walrusStatistics = null;

    private static String strSBXContentTypeFolder = "application/x-directory";

    private long getSyncId(String eventType, String account) throws EucalyptusCloudException {
		long syncid = -1;

		if (eventType == null)
			throw new AccessDeniedException("GetSyncIdError", account + ": no client type", true);
		
		if ("webApp".equals(eventType)) {
			syncid = 0;
		} else if ("mobileApp".equals(eventType)) {
			syncid = 0;
		} else {
			syncid = Long.valueOf(eventType);
			if (syncid <= 0)
				throw new AccessDeniedException("GetSyncIdError", account + ": error syncid=" + syncid + "(<=0)", true);
		}
		return syncid;
	}

	private void writeEvent(EntityManager em,final String eventType,final String account,final String opt,final
			String object_key,final Long size,final Boolean isFolder,final long syncid,final long lastModified, final String reqid, final int objseq) throws EucalyptusCloudException {
		
		Session sess = null;
		try {
			sess = ((Session)em.getDelegate());
		} catch (Throwable t) {	
			sess = null;
		}

        final java.sql.Timestamp mtime = new java.sql.Timestamp(lastModified);

        if ("webApp".equals(eventType)) {
			final String sql = "insert into web_opt_log(id,rectime,time,cssact,op,fpth,fsz,file_version,syncid,isfolder,rslt,inst_id, reqid) values(DEFAULT,CURRENT_TIMESTAMP AT TIME ZONE 'UTC +0',?,?,?,?,?,?,NULL,?,'succ','dummy',?)";
			if (sess != null) {
				sess.doWork(
					    new Work() {
					        public void execute(Connection connection) throws SQLException {
                                PreparedStatement stmt = null;
                                try {
                                    stmt=connection.prepareStatement(sql);
                                    stmt.setTimestamp(1, mtime);
                                    stmt.setString(2, account);
                                    stmt.setString(3, opt);
                                    stmt.setString(4, object_key);
                                    stmt.setLong(5, size);
                                    stmt.setInt(6, objseq);
                                    stmt.setBoolean(7, isFolder);
                                    stmt.setString(8, reqid);
                                    stmt.executeUpdate();
                                } finally {
                                    if (stmt != null) stmt.close();
                                }
					        }
					    }
					);					
			} else {
				em.createNativeQuery(sql)
				.setParameter(1, mtime)
				.setParameter(2, account)
				.setParameter(3, opt)
				.setParameter(4, object_key)
				.setParameter(5, size)
                .setParameter(6, objseq)
				.setParameter(7, isFolder)
                .setParameter(8, reqid)
				.executeUpdate();
			}
		} else if ("mobileApp".equals(eventType)) {
		} else {
			final String sql = "insert into opt_log(id,rectime,time,cssact,op,fpth,fsz,file_version,syncid,isfolder,rslt,inst_id, reqid) values(DEFAULT,CURRENT_TIMESTAMP AT TIME ZONE 'UTC +0',?,?,?,?,?,?,?,?,'succ','dummy',?)";
			if (sess != null) {
				sess.doWork(
                        new Work() {
                            public void execute(Connection connection) throws SQLException {
                                PreparedStatement stmt = null;
                                try {
                                    stmt = connection.prepareStatement(sql);
                                    stmt.setTimestamp(1, mtime);
                                    stmt.setString(2, account);
                                    stmt.setString(3, opt);
                                    stmt.setString(4, object_key);
                                    stmt.setLong(5, size);
                                    stmt.setInt(6, objseq);
                                    stmt.setLong(7, syncid);
                                    stmt.setBoolean(8, isFolder);
                                    stmt.setString(9, reqid);
                                    stmt.executeUpdate();
                                } finally {
                                    if (stmt != null) stmt.close();
                                }

                                }
                            }

                            );
                        }else {
				em.createNativeQuery(sql)
				.setParameter(1, mtime)
				.setParameter(2, account)
				.setParameter(3, opt)
				.setParameter(4, object_key)
				.setParameter(5, size)
                .setParameter(6, objseq)
				.setParameter(7, syncid)
				.setParameter(8, isFolder)
                .setParameter(9, reqid)
                .executeUpdate();
			}
		}
	}
	private void writeRenameEvent(EntityManager em,final String eventType,final String account,final String bucketName,
			final String object_key,final String renameTo,final long syncid, final String reqid, final int objseq) throws EucalyptusCloudException {
		
		Session sess = null;
		try {
			sess = ((Session)em.getDelegate());
		} catch (Throwable t) {	
			sess = null;
		}

		if ("webApp".equals(eventType)) {
			final String sql = "insert into web_opt_log(rectime,time,cssact,op,fpth,fsz,file_version,syncid,isfolder,rslt,inst_id,reqid)"+
					   "select ?,to_timestamp(coalesce(value::bigint,0)/1000),?,'Rename',?,size,?,?,content_type='application/x-directory','succ','dummy',? from objects "+
					   "obj left join metadata mtd on obj.object_name=mtd.object_id and mtd.\"name\"='mtime' "+
					   "where object_key = ? AND bucket_name = ? AND owner_id = ? limit 1";
	
			if (sess != null) {
				sess.doWork(
					    new Work() {
					        public void execute(Connection connection) throws SQLException {
                                PreparedStatement stmt = null;
                                try {
                                    stmt=connection.prepareStatement(sql);
                                    Calendar c = Calendar.getInstance();
                                    stmt.setTimestamp(1, new Timestamp(c.getTimeInMillis() - c.getTimeZone().getRawOffset()));
                                    stmt.setString(2, account);
                                    stmt.setString(3,object_key+"||"+renameTo);
                                    stmt.setInt(4, objseq);
                                    stmt.setLong(5, syncid);
                                    stmt.setString(6, reqid);
                                    stmt.setString(7,renameTo);
                                    stmt.setString(8,bucketName);
                                    stmt.setString(9,account);
                                    stmt.executeUpdate();
                                } finally {
                                    try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
                                }
					        }
					    }
					);					
			} else {			
				Calendar c = Calendar.getInstance();
                em.createNativeQuery(sql)
                        .setParameter(1, new Timestamp(c.getTimeInMillis() - c.getTimeZone().getRawOffset()))
                        .setParameter(2, account)
                        .setParameter(3, object_key+"||"+renameTo)
                        .setParameter(4, objseq)
                        .setParameter(5, syncid)
                        .setParameter(6, reqid)
                        .setParameter(7, renameTo)
                        .setParameter(8, bucketName)
                        .setParameter(9, account)
                        .executeUpdate();
			}

		} else if ("mobileApp".equals(eventType)) {
		} else {
			
			final String sql="insert into opt_log(rectime,time,cssact,op,fpth,fsz,file_version,syncid,isfolder,rslt,inst_id,reqid) "+
					   "select ?,to_timestamp(coalesce(value::bigint,0)/1000),?,'Rename',?,size,?,?,content_type='application/x-directory','succ','dummy',? from objects "+
					   "obj left join metadata mtd on obj.object_name=mtd.object_id and mtd.\"name\"='mtime' "+
					   "where object_key = ? AND bucket_name = ? AND owner_id = ? limit 1";

			if (sess != null) {
				sess.doWork(
					    new Work() {
					        public void execute(Connection connection) throws SQLException {
                                PreparedStatement stmt = null;
                                try {
                                    stmt=connection.prepareStatement(sql);
                                    Calendar c = Calendar.getInstance();
                                    stmt.setTimestamp(1, new Timestamp(c.getTimeInMillis() - c.getTimeZone().getRawOffset()));
                                    stmt.setString(2, account);
                                    stmt.setString(3,object_key+"||"+renameTo);
                                    stmt.setInt(4, objseq);
                                    stmt.setLong(5, syncid);
                                    stmt.setString(6, reqid);
                                    stmt.setString(7,renameTo);
                                    stmt.setString(8,bucketName);
                                    stmt.setString(9,account);
                                    stmt.executeUpdate();
                                } finally {
                                    try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
                                }
					        }
					    }
					);					
			} else {
                Calendar c = Calendar.getInstance();
                em.createNativeQuery(sql)
                        .setParameter(1, new Timestamp(c.getTimeInMillis() - c.getTimeZone().getRawOffset()))
                        .setParameter(2, account)
                        .setParameter(3, object_key+"||"+renameTo)
                        .setParameter(4, objseq)
                        .setParameter(5, syncid)
                        .setParameter(6, reqid)
                        .setParameter(7, renameTo)
                        .setParameter(8, bucketName)
                        .setParameter(9, account)
                        .executeUpdate();
			}
		}
	}
    private void doRenameEvent(EntityManager em,final String account,final String bucketName,
                                  final String object_key,final String renameTo) throws EucalyptusCloudException {

        Session sess = null;
        try {
            sess = ((Session)em.getDelegate());
        } catch (Throwable t) {
            sess = null;
        }

        final String sql="UPDATE objects SET object_key=regexp_replace(object_key, ?, ?, 'q') " +
                "where bucket_name = ? and owner_id = ? and (object_key LIKE ? or object_key = ?)";

        final String like_key = object_key.replaceAll("%", "\\%").replaceAll("_", "\\_");
        if (sess != null) {
            sess.doWork(
                    new Work() {
                        public void execute(Connection connection) throws SQLException {
                            PreparedStatement stmt = null;
                            try {
                                stmt=connection.prepareStatement(sql);
                                stmt.setString(1, object_key);
                                stmt.setString(2, renameTo);
                                stmt.setString(3, bucketName);
                                stmt.setString(4, account);
                                //stmt.setString(5, "^" + object_key + "/");
                                stmt.setString(5, like_key + "/%");
                                stmt.setString(6, object_key);
                                int exR = stmt.executeUpdate();
                                if(exR==0){
                                    throw new SQLException("CssError", account + ":" + bucketName + ":" + object_key + "***" + "no objects are renamed");
                                }
                            } finally {
                                try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
                            }
                        }
                    }
            );
        } else {
            int exR = em.createNativeQuery(sql)
                    .setParameter(1, object_key)
                    .setParameter(2, renameTo)
                    .setParameter(3, bucketName)
                    .setParameter(4, account)
                    //.setParameter(5, "^" + object_key + "/")
                    .setParameter(5, like_key + "/%")
                    .setParameter(6, object_key)
                    .executeUpdate();
            if(exR==0){
                throw new AccessDeniedException("CssError", account + ":" + bucketName + ":" + object_key + "***" + "no objects are renamed", true);
            }
        }
    }

    public static void callWalrusHeartBeat(String account, String instid, String API) throws Throwable {
        HttpClient client = null;
        HttpMethod method = null;
        NameValuePair[] queryString = null;
        if (account == null || instid == null) {
            LOG.debug(API + ":callWalrusHeartBeat error: #account=" + account + "#instid=" + instid + "#");
            return;
        }
        try {
            client = new HttpClient();
            String URL = "http://127.0.0.1/sbx_svr/rest/EBS/walrusheartbeat";
            queryString = new NameValuePair[]{new NameValuePair("account", account), new NameValuePair("instanceid", instid)};
            method = new PostMethod(URL);
            method.addRequestHeader(new Header("Connection", "close"));
            method.setQueryString(queryString);
            int statusCode = client.executeMethod(method);
            if (statusCode == HttpStatus.SC_OK) {
                // Read the response body.
                StringBuffer stb = new StringBuffer();
                InputStream ins = method.getResponseBodyAsStream();
                InputStreamReader insReader = new InputStreamReader(ins);
                BufferedReader br = new BufferedReader(insReader);
                String buffText = br.readLine();
                while (null != buffText) {
                    stb.append(buffText);
                    buffText = br.readLine();
                }
                if (stb.length() == 0 || StringUtils.isEmpty(stb.toString())) {
                    LOG.debug(API + ":callWalrusHeartBeat: Http Response Body is empty!");
                }
            } else {
                LOG.debug(API + ":callWalrusHeartBeat: Http Response Error:" + statusCode + "#account=" + account + "#instid=" + instid);
            }
        } catch (Throwable t) {
            LOG.debug(API + ":callWalrusHeartBeat: Http Response Error: #account=" + account + "#instid=" + instid + "#" + t.toString());
            throw t;
        } finally {
            try { if (method != null) method.releaseConnection(); } catch (Throwable t) {/*NOP*/};
        }
    }

	public static void configure() {
		walrusStatistics = new WalrusStatistics();
	}

	public WalrusManager(StorageManager storageManager, WalrusImageManager walrusImageManager) {
		this.storageManager = storageManager;
		this.walrusImageManager = walrusImageManager;
	}

	public void initialize() throws EucalyptusCloudException {
		check();
	}

	public void check() throws EucalyptusCloudException {
		File bukkitDir = new File(WalrusInfo.getWalrusInfo().getStorageDir());
		if (!bukkitDir.exists()) {
			if (!bukkitDir.mkdirs()) {
				LOG.fatal("Unable to make bucket root directory: "
						+ WalrusInfo.getWalrusInfo().getStorageDir());
				throw new EucalyptusCloudException("Invalid bucket root directory");
			}
		} else if (!bukkitDir.canWrite()) {
			LOG.fatal("Cannot write to bucket root directory: " + WalrusInfo.getWalrusInfo().getStorageDir());
			throw new EucalyptusCloudException("Invalid bucket root directory");
		}
		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo();
		List<BucketInfo> bucketInfos = db.query(bucketInfo);
		for (BucketInfo bucket : bucketInfos) {
			if (!storageManager.bucketExists(bucket.getBucketName()))
				bucket.setHidden(true);
			else
				bucket.setHidden(false);
		}
		db.commit();
	}

    static Cache<String, ArrayList<BucketListEntry>> cache_buckets = CacheBuilder.newBuilder().maximumSize(1000).build();
    static Cache<String, CanonicalUserType> cache_owner = CacheBuilder.newBuilder().maximumSize(1000).build();
	public ListAllMyBucketsResponseType listAllMyBuckets(ListAllMyBucketsType request)
			throws EucalyptusCloudException {
		ListAllMyBucketsResponseType reply = (ListAllMyBucketsResponseType) request.getReply();
		String userId = request.getUserId();

		if (userId == null) {
			throw new AccessDeniedException("no such user");
		}

        ArrayList<BucketListEntry> buckets = null;
        CanonicalUserType owner = null;

        // cache
        boolean bCache = false;
        String key_buckets = userId + "_buckets";
        String key_owner = userId + "_owner";
        if (Contexts.SBXSVR) {
            try {
                buckets = cache_buckets.getIfPresent(key_buckets);
            } catch (Throwable t) {
                LOG.error("Cache error: " + t);
            }
            try {
                owner = cache_owner.getIfPresent(key_owner);
            } catch (Throwable t) {
                LOG.error("Cache error: " + t);
            }

            if (buckets == null) {
                EntityWrapper<BucketInfo> db = null;
                try {
                    db = WalrusControl.getEntityWrapper();
                    BucketInfo searchBucket = new BucketInfo();
                    searchBucket.setOwnerId(userId);
                    searchBucket.setHidden(false);
                    List<BucketInfo> bucketInfoList = db.query(searchBucket);

                    buckets = new ArrayList<BucketListEntry>();
                    for (BucketInfo bucketInfo : bucketInfoList) {
                        if (request.isAdministrator()) {
                            EntityWrapper<WalrusSnapshotInfo> dbSnap = db.recast(WalrusSnapshotInfo.class);
                            WalrusSnapshotInfo walrusSnapInfo = new WalrusSnapshotInfo();
                            walrusSnapInfo.setSnapshotBucket(bucketInfo.getBucketName());
                            List<WalrusSnapshotInfo> walrusSnaps = dbSnap.query(walrusSnapInfo);
                            if (walrusSnaps.size() > 0)
                                continue;
                        }
                        buckets.add(new BucketListEntry(bucketInfo.getBucketName(), DateUtils.format(bucketInfo
                                .getCreationDate().getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
                                + ".000Z"));
                    }
                    db.commit();
                } catch (Throwable t) {
                    if (db != null) db.rollback();
                    throw new AccessDeniedException("CssError", userId + "***" + t.toString(), true);
                }
                try {
                    if (buckets.size() > 0)
                        cache_buckets.put(key_buckets, buckets);
                } catch (Throwable t) {
                    LOG.error("Cache error: " + t);
                }
            }

            if (owner == null) {
                try {
                    owner = new CanonicalUserType(Users.lookupUser(userId).getQueryId(), userId);
                } catch (Exception ex) {
                    throw new AccessDeniedException("User: " + userId + " not found");
                } catch (Throwable t) {
                    throw new AccessDeniedException("CssError", userId + "***" + t.toString(), true);
                }
                try {
                    cache_owner.put(key_owner, owner);
                } catch (Throwable t) {
                    LOG.error("Cache error: " + t);
                }
            }

            try {
                ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
                reply.setOwner(owner);
                bucketList.setBuckets(buckets);
                reply.setBucketList(bucketList);
            } catch (Throwable t) {
                throw new AccessDeniedException("CssError", userId + "***" + t.toString(), true);
            }
            return reply;
        }

        try {
            EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
            BucketInfo searchBucket = new BucketInfo();
            searchBucket.setOwnerId(userId);
            searchBucket.setHidden(false);
            List<BucketInfo> bucketInfoList = db.query(searchBucket);

            buckets = new ArrayList<BucketListEntry>();
            for (BucketInfo bucketInfo : bucketInfoList) {
                if (request.isAdministrator()) {
                    EntityWrapper<WalrusSnapshotInfo> dbSnap = db.recast(WalrusSnapshotInfo.class);
                    WalrusSnapshotInfo walrusSnapInfo = new WalrusSnapshotInfo();
                    walrusSnapInfo.setSnapshotBucket(bucketInfo.getBucketName());
                    List<WalrusSnapshotInfo> walrusSnaps = dbSnap.query(walrusSnapInfo);
                    if (walrusSnaps.size() > 0)
                        continue;
                }
                buckets.add(new BucketListEntry(bucketInfo.getBucketName(), DateUtils.format(bucketInfo
                        .getCreationDate().getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
                        + ".000Z"));
            }
            try {
                owner = new CanonicalUserType(Users.lookupUser(userId).getQueryId(), userId);

                ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
                reply.setOwner(owner);
                bucketList.setBuckets(buckets);
                reply.setBucketList(bucketList);
            } catch (Exception ex) {
                db.rollback();
                LOG.error(ex);
                throw new AccessDeniedException("User: " + userId + " not found");
            }
            db.commit();
        } catch (AccessDeniedException e) {
            throw e;
        } catch (Throwable t) {
            throw new AccessDeniedException("CssError", userId + "***" + t.toString(), true);
        }
		return reply;
	}

	public CreateBucketResponseType createBucket(CreateBucketType request) throws EucalyptusCloudException {
		CreateBucketResponseType reply = (CreateBucketResponseType) request.getReply();
		String userId = request.getUserId();

		String bucketName = request.getBucket();
		String locationConstraint = request.getLocationConstraint();

		if (userId == null) {
			throw new AccessDeniedException("Bucket", bucketName);
		}

        if (Contexts.SBXSVR) {
            if (bucketName.equals("bkt-")) {
                throw new InvalidBucketNameException(bucketName);
            }
        }

		AccessControlListType accessControlList = request.getAccessControlList();
		if (accessControlList == null) {
			accessControlList = new AccessControlListType();
		}

		if (!checkBucketName(bucketName))
			throw new InvalidBucketNameException(bucketName);

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();

		if (WalrusProperties.shouldEnforceUsageLimits && !request.isAdministrator()) {
			BucketInfo searchBucket = new BucketInfo();
			searchBucket.setOwnerId(userId);
			List<BucketInfo> bucketList = db.query(searchBucket);
			if (bucketList.size() >= WalrusInfo.getWalrusInfo().getStorageMaxBucketsPerUser()) {
				db.rollback();
				throw new TooManyBucketsException(bucketName);
			}
		}

		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			if (bucketList.get(0).getOwnerId().equals(userId)) {
				// bucket already exists and you created it
				db.rollback();
				throw new BucketAlreadyOwnedByYouException(bucketName);
			}
			// bucket already exists
			db.rollback();
			throw new BucketAlreadyExistsException(bucketName);
		} else {
			// create bucket and set its acl
			BucketInfo bucket = new BucketInfo(userId, bucketName, new Date());
			ArrayList<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
			bucket.addGrants(userId, grantInfos, accessControlList);
			bucket.setGrants(grantInfos);
			bucket.setBucketSize(0L);
			bucket.setLoggingEnabled(false);
			bucket.setVersioning(WalrusProperties.VersioningStatus.Disabled.toString());
			bucket.setHidden(false);
			if (locationConstraint != null)
				bucket.setLocation(locationConstraint);
			else
				bucket.setLocation("US");
			db.add(bucket);
			// call the storage manager to save the bucket to disk
			try {
				storageManager.createBucket(bucketName);
				if (WalrusProperties.trackUsageStatistics)
					walrusStatistics.incrementBucketCount();
			} catch (IOException ex) {
				LOG.error(ex);
				db.rollback();
				throw new AccessDeniedException("Unable to create bucket: " + bucketName);
			}
		}
		db.commit();

		if (WalrusProperties.enableVirtualHosting) {
			if (checkDNSNaming(bucketName)) {
				UpdateARecordType updateARecord = new UpdateARecordType();
				updateARecord.setUserId(userId);
				URI walrusUri;
				String address = null;
				try {
					walrusUri = new URI(SystemConfiguration.getWalrusUrl());
					address = walrusUri.getHost();
				} catch (URISyntaxException e) {
					throw new AccessDeniedException("Could not get Walrus URL");
				}
				String zone = WalrusProperties.WALRUS_SUBDOMAIN + ".";
				updateARecord.setAddress(address);
				updateARecord.setName(bucketName + "." + zone);
				updateARecord.setTtl(604800);
				updateARecord.setZone(zone);
				try {
					ServiceDispatcher.lookupSingle(Component.dns).send(updateARecord);
					LOG.info("Mapping " + updateARecord.getName() + " to " + address);
				} catch (Exception ex) {
					LOG.error("Could not update DNS record", ex);
				}
			} else {
				LOG.error("Bucket: " + bucketName
						+ " fails to meet DNS requirements. Unable to create DNS mapping.");
			}
		}

		reply.setBucket(bucketName);
		return reply;
	}

	private boolean checkBucketName(String bucketName) {
		if (!(bucketName.matches("^[A-Za-z0-9].*") || bucketName.contains(".") || bucketName.contains("-")))
			return false;
		if (bucketName.length() < 3 || bucketName.length() > 255)
			return false;
		String[] addrParts = bucketName.split("\\.");
		boolean ipFormat = true;
		if (addrParts.length == 4) {
			for (String addrPart : addrParts) {
				try {
					Integer.parseInt(addrPart);
				} catch (NumberFormatException ex) {
					ipFormat = false;
					break;
				}
			}
		} else {
			ipFormat = false;
		}
		if (ipFormat)
			return false;
		return true;
	}

	private boolean checkDNSNaming(String bucketName) {
		if (bucketName.contains("_"))
			return false;
		if (bucketName.length() < 3 || bucketName.length() > 63)
			return false;
		if (bucketName.endsWith("-"))
			return false;
		if (bucketName.contains("-.") || bucketName.contains(".-"))
			return false;
		return true;
	}

	public DeleteBucketResponseType deleteBucket(DeleteBucketType request) throws EucalyptusCloudException {
		DeleteBucketResponseType reply = (DeleteBucketResponseType) request.getReply();
		String bucketName = request.getBucket();
		String userId = request.getUserId();
		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo searchBucket = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(searchBucket);

		if (bucketList.size() > 0) {
			BucketInfo bucketFound = bucketList.get(0);
			BucketLogData logData = bucketFound.getLoggingEnabled() ? request.getLogData() : null;
			if (bucketFound.canWrite(userId)) {
				EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
				ObjectInfo searchObject = new ObjectInfo();
				searchObject.setBucketName(bucketName);
				searchObject.setDeleted(false);
				List<ObjectInfo> objectInfos = dbObject.query(searchObject);
				if (objectInfos.size() == 0) {
					// check if the bucket contains any images
					EntityWrapper<ImageCacheInfo> dbIC = db.recast(ImageCacheInfo.class);
					ImageCacheInfo searchImageCacheInfo = new ImageCacheInfo();
					searchImageCacheInfo.setBucketName(bucketName);
					List<ImageCacheInfo> foundImageCacheInfos = dbIC.query(searchImageCacheInfo);

					if (foundImageCacheInfos.size() > 0) {
						db.rollback();
						throw new BucketNotEmptyException(bucketName, logData);
					}
					// remove any delete markers
					ObjectInfo searchDeleteMarker = new ObjectInfo();
					searchDeleteMarker.setBucketName(bucketName);
					searchDeleteMarker.setDeleted(true);
					List<ObjectInfo> deleteMarkers = dbObject.query(searchDeleteMarker);
					for (ObjectInfo deleteMarker : deleteMarkers) {
						dbObject.delete(deleteMarker);
					}
					db.delete(bucketFound);
					// Actually remove the bucket from the backing store
					try {
						storageManager.deleteBucket(bucketName);
						if (WalrusProperties.trackUsageStatistics)
							walrusStatistics.decrementBucketCount();
					} catch (IOException ex) {
						// set exception code in reply
						LOG.error(ex);
					}

					if (WalrusProperties.enableVirtualHosting) {
						URI walrusUri;
						String address;
						RemoveARecordType removeARecordType = new RemoveARecordType();
						removeARecordType.setUserId(userId);
						String zone = WalrusProperties.WALRUS_SUBDOMAIN + ".";
						removeARecordType.setName(bucketName + "." + zone);
						removeARecordType.setZone(zone);
						try {
							walrusUri = new URI(SystemConfiguration.getWalrusUrl());
							address = walrusUri.getHost();
						} catch (URISyntaxException e) {
							db.rollback();
							throw new AccessDeniedException("Could not get Walrus URL");
						}
						removeARecordType.setAddress(address);
						try {
							ServiceDispatcher.lookupSingle(Component.dns).send(removeARecordType);
							LOG.info("Removing mapping for " + removeARecordType.getName());
						} catch (Exception ex) {
							LOG.error("Could not update DNS record", ex);
						}
					}

					Status status = new Status();
					status.setCode(204);
					status.setDescription("No Content");
					reply.setStatus(status);
					if (logData != null) {
						updateLogData(bucketFound, logData);
						reply.setLogData(logData);
					}
				} else {
					db.rollback();
					throw new BucketNotEmptyException(bucketName, logData);
				}
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public GetBucketAccessControlPolicyResponseType getBucketAccessControlPolicy(
			GetBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		GetBucketAccessControlPolicyResponseType reply = (GetBucketAccessControlPolicyResponseType) request
				.getReply();

		String bucketName = request.getBucket();
		String userId = request.getUserId();
		String ownerId = null;

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		AccessControlListType accessControlList = new AccessControlListType();
		BucketLogData logData;

		if (bucketList.size() > 0) {
			// construct access control policy from grant infos
			BucketInfo bucket = bucketList.get(0);
			logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			List<GrantInfo> grantInfos = bucket.getGrants();
			if (bucket.canReadACP(userId)) {
				if (logData != null) {
					updateLogData(bucket, logData);
					reply.setLogData(logData);
				}
				ownerId = bucket.getOwnerId();
				ArrayList<Grant> grants = new ArrayList<Grant>();
				bucket.readPermissions(grants);
				for (GrantInfo grantInfo : grantInfos) {
					String uId = grantInfo.getUserId();
					try {
						if (uId != null) {
							User grantUserInfo = Users.lookupUser(uId);
							addPermission(grants, grantUserInfo, grantInfo);
						} else {
							addPermission(grants, grantInfo);
						}
					} catch (NoSuchUserException e) {
						db.rollback();
						throw new AccessDeniedException("Bucket", bucketName, logData);
					}
				}
				accessControlList.setGrants(grants);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}

		AccessControlPolicyType accessControlPolicy = new AccessControlPolicyType();
		try {
			User ownerUserInfo = Users.lookupUser(ownerId);
			accessControlPolicy.setOwner(new CanonicalUserType(ownerUserInfo.getQueryId(), ownerUserInfo
					.getName()));
			accessControlPolicy.setAccessControlList(accessControlList);
		} catch (NoSuchUserException e) {
			db.rollback();
			throw new AccessDeniedException("Bucket", bucketName, logData);
		}
		reply.setAccessControlPolicy(accessControlPolicy);
		db.commit();
		return reply;
	}

	private static void addPermission(ArrayList<Grant> grants, User userInfo, GrantInfo grantInfo) {
		CanonicalUserType user = new CanonicalUserType(userInfo.getQueryId(), userInfo.getName());

		if (grantInfo.canRead() && grantInfo.canWrite() && grantInfo.canReadACP() && grantInfo.isWriteACP()) {
			grants.add(new Grant(new Grantee(user), "FULL_CONTROL"));
			return;
		}

		if (grantInfo.canRead()) {
			grants.add(new Grant(new Grantee(user), "READ"));
		}

		if (grantInfo.canWrite()) {
			grants.add(new Grant(new Grantee(user), "WRITE"));
		}

		if (grantInfo.canReadACP()) {
			grants.add(new Grant(new Grantee(user), "READ_ACP"));
		}

		if (grantInfo.isWriteACP()) {
			grants.add(new Grant(new Grantee(user), "WRITE_ACP"));
		}
	}

	private static void addPermission(ArrayList<Grant> grants, GrantInfo grantInfo) {
		if (grantInfo.getGrantGroup() != null) {
			Group group = new Group(grantInfo.getGrantGroup());

			if (grantInfo.canRead() && grantInfo.canWrite() && grantInfo.canReadACP()
					&& grantInfo.isWriteACP()) {
				grants.add(new Grant(new Grantee(group), "FULL_CONTROL"));
				return;
			}

			if (grantInfo.canRead()) {
				grants.add(new Grant(new Grantee(group), "READ"));
			}

			if (grantInfo.canWrite()) {
				grants.add(new Grant(new Grantee(group), "WRITE"));
			}

			if (grantInfo.canReadACP()) {
				grants.add(new Grant(new Grantee(group), "READ_ACP"));
			}

			if (grantInfo.isWriteACP()) {
				grants.add(new Grant(new Grantee(group), "WRITE_ACP"));
			}
		}
	}
	protected PutObjectResponseType renameObject(PutObjectType request, SbxRequest r) throws WalrusException{
		PutObjectResponseType reply = (PutObjectResponseType) request.getReply();

		String userId = request.getUserId(),
			   bucketName = request.getBucket(),
			   objectKey = request.getKey(),
			   renameTo=request.getRenameto(),
			   account=request.getAccount(),
			   instanceId=request.getInstanceid();

        if (Contexts.SBXSVR) {
            try {
                String myaccount = userId;
                String myinstid = instanceId;
                WalrusControl.walrusManager.callWalrusHeartBeat(myaccount, myinstid, "RENAMEOBJECT");
            } catch (Throwable t) {
                Log.debug("CSS callWalrusHeartBeat error (renameObject) " + "***" + t.toString());
            }
        }

        long syncid = -1;
        if (Contexts.SBXSVR) {
            // mac: get sync id before putObject
            try {
                syncid = this.getSyncId(request.getMetaClientType(), userId);
                // syncid must LOCK (syncid == 0 for mobile and browser)
                if (syncid > 0 && r != null && r.reqId != null) {
                    if (SbxDB.syncidlock(syncid) == false) {
                        throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "*** syncid=" + syncid + " need be locked", true);
                    }
                }
            } catch (AccessDeniedException ex) {
                throw ex;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
            }
        }

        int objSeq = -1;
        if (Contexts.SBXSVR) {
            try {
                if (r != null && StringUtils.isNotEmpty(r.objSeq)) {
                    objSeq = Integer.parseInt(r.objSeq);
                }
                if (objSeq != -1) {
                    // compare objSeq with Object's obj-seq
                    int dbObjSeq = SbxDB.getObjSeq(userId, bucketName, objectKey);
                    if (objSeq != dbObjSeq) {
                        throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "#obj-seq are different: " + objSeq + " != " + dbObjSeq, true);
                    }
                }
            } catch (AccessDeniedException t) {
                throw t;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
            }
        }

        String md5 = "";
		Date lastModified = new Date();
		long size=0;
		AccessControlListType accessControlList = request.getAccessControlList();
		if (accessControlList == null) {accessControlList = new AccessControlListType();}

		
		//====empty and remove data queue
		String key = bucketName + "." + objectKey;
		String randomKey = request.getRandomKey();
		WalrusDataMessenger messenger = WalrusRESTBinding.getWriteMessenger();
		WalrusDataQueue<WalrusDataMessage> putQueue = messenger.getQueue(key, randomKey);
        putQueue.clear();
		messenger.removeQueue(key, randomKey);
		//====		
		
		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		EntityWrapper<ObjectInfo> dbObject=db.recast(ObjectInfo.class);
		try {
			

			List<BucketInfo> bucketList = db.query(new BucketInfo(bucketName));
			if(bucketList.size()<=0){throw new NoSuchBucketException(bucketName);}
			
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if(!bucket.canWrite(userId)){throw new AccessDeniedException("Bucket", bucketName, logData);}

            String objectName;
			String versionId;
			ObjectInfo objectInfo = null;
			if (bucket.isVersioningEnabled()) {//new an object by hibernate
                Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error rename does not support version");
                throw new AccessDeniedException("NotSupportVersion", userId + ":" + bucketName + ":" + objectKey + "***" + "Error rename does not support version", true);
			} else {//modify object
						
						/*
						versionId = WalrusProperties.NULL_VERSION_ID;
						ObjectInfo searchObject = new ObjectInfo(bucketName, objectKey);
								   searchObject.setVersionId(versionId);

						ObjectInfo sourceObject=null,targetObject=new ObjectInfo(bucketName,renameTo);
					    
					    //=======to check source exsit and target doesn't.
					    try{
					    	sourceObject = dbObject.getUnique(searchObject);
					    }catch(EucalyptusCloudException ec){throw new NoSuchEntityException(objectKey);}  
					    if (!sourceObject.canWrite(userId)) {throw new AccessDeniedException("Key", objectKey, logData);}
					    
					    //===========
					    List tmpL=dbObject.query(targetObject);int tmpLsize=tmpL.size();tmpL.clear();
					    if(tmpLsize!=0){throw new AccessDeniedException("Key",renameTo,logData);};
					    //==========*/


//                String sql="UPDATE objects SET object_key=regexp_replace(object_key, ?, ?, 'q') " +
//                        "where bucket_name = ? and owner_id = ? and (object_key ~ ? or object_key = ?)";
//
//                SQLQuery query=dbObject.getSession().createSQLQuery(sql);
//                query.setString(0, objectKey);
//                query.setString(1, renameTo);
//                query.setString(2, bucketName);
//                query.setString(3, account);
//                query.setString(4, "^" + objectKey + "/");
//                query.setString(5, objectKey);
//                int exR=query.executeUpdate();
//
//                if(exR==0){
//                    Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error rename in sql");
//                    throw new AccessDeniedException("SqlError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error rename in sql", true);

                    doRenameEvent(dbObject.getEntityManager(), account, bucketName, objectKey, renameTo);
			}

            if (Contexts.SBXSVR) {
                String reqid = request.getMetaReqId();
                if (reqid == null) reqid = "null";
                if (objSeq == -1) {
                    objSeq = 0;
                }
                try {
                    writeRenameEvent(dbObject.getEntityManager(),instanceId,account,bucketName,objectKey,renameTo,syncid,reqid,objSeq);
                }catch (Throwable t) {
                    throw new AccessDeniedException("WriteRenameEventError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error rename in sql", true);
                }
            }

            db.commit();
						
			reply.setLogData(logData);
			reply.setEtag(md5);
			reply.setSize(size);
			reply.setLastModified(DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)+ ".000Z");
			
			
		}catch(AccessDeniedException we){
			db.rollback();
			throw we;
		}catch(Throwable t){
            Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error" + "***" + t);
			db.rollback();
			throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error" + "***" + t.getMessage(), true);
		}finally {}
		return reply;
		
		
	}

	public PostObjectResponseType postObject(PostObjectType request) throws EucalyptusCloudException {
		PostObjectResponseType reply = (PostObjectResponseType) request.getReply();

		String bucketName = request.getBucket();
		String key = request.getKey();

		PutObjectType putObject = new PutObjectType();
		putObject.setUserId(request.getUserId());
		putObject.setBucket(bucketName);
		putObject.setKey(key);
		putObject.setRandomKey(request.getRandomKey());
		putObject.setAccessControlList(request.getAccessControlList());
		putObject.setContentType(request.getContentType());
		putObject.setContentLength(request.getContentLength());
		putObject.setAccessKeyID(request.getAccessKeyID());
		putObject.setEffectiveUserId(request.getEffectiveUserId());
		putObject.setCredential(request.getCredential());
		putObject.setIsCompressed(request.getIsCompressed());
		putObject.setMetaData(request.getMetaData());
		putObject.setStorageClass(request.getStorageClass());

		PutObjectResponseType putObjectResponse = putObjectRefactor(putObject);

		String etag = putObjectResponse.getEtag();
		reply.setEtag(etag);
		reply.setLastModified(putObjectResponse.getLastModified());
		reply.set_return(putObjectResponse.get_return());
		reply.setMetaData(putObjectResponse.getMetaData());
		reply.setErrorCode(putObjectResponse.getErrorCode());
		reply.setStatusMessage(putObjectResponse.getStatusMessage());
		reply.setLogData(putObjectResponse.getLogData());

		String successActionRedirect = request.getSuccessActionRedirect();
		if (successActionRedirect != null) {
			try {
				java.net.URI addrUri = new URL(successActionRedirect).toURI();
				InetAddress.getByName(addrUri.getHost());
			} catch (Exception ex) {
				LOG.warn(ex);
			}
			String paramString = "bucket=" + bucketName + "&key=" + key + "&etag=quot;" + etag + "quot;";
			reply.setRedirectUrl(successActionRedirect + "?" + paramString);
		} else {
			Integer successActionStatus = request.getSuccessActionStatus();
			if (successActionStatus != null) {
				if ((successActionStatus == 200) || (successActionStatus == 201)) {
					reply.setSuccessCode(successActionStatus);
					if (successActionStatus == 200) {
						return reply;
					} else {
						reply.setBucket(bucketName);
						reply.setKey(key);
						reply.setLocation(SystemConfiguration.getWalrusUrl() + "/" + bucketName + "/" + key);
					}
				} else {
					reply.setSuccessCode(204);
					return reply;
				}
			}
		}
		return reply;
	}

	public PutObjectInlineResponseType putObjectInline(PutObjectInlineType request)
			throws EucalyptusCloudException {
		PutObjectInlineResponseType reply = (PutObjectInlineResponseType) request.getReply();
		String userId = request.getUserId();

		String bucketName = request.getBucket();
		String objectKey = request.getKey();

		String md5 = "";
		Long oldBucketSize = 0L;
		Date lastModified;

		AccessControlListType accessControlList = request.getAccessControlList();
		if (accessControlList == null) {
			accessControlList = new AccessControlListType();
		}

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if (bucket.canWrite(userId)) {
				EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
				ObjectInfo searchObjectInfo = new ObjectInfo();
				searchObjectInfo.setBucketName(bucketName);

				ObjectInfo foundObject = null;
				List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
				for (ObjectInfo objectInfo : objectInfos) {
					if (objectInfo.getObjectKey().equals(objectKey)) {
						// key (object) exists. check perms
						if (!objectInfo.canWrite(userId)) {
							db.rollback();
							throw new AccessDeniedException("Key", objectKey, logData);
						}
						foundObject = objectInfo;
						oldBucketSize = -foundObject.getSize();
						break;
					}
				}
				// write object to bucket
				String objectName;
				if (foundObject == null) {
					// not found. create an object info
					foundObject = new ObjectInfo(bucketName, objectKey);
					foundObject.setOwnerId(userId);
					List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
					foundObject.addGrants(userId, grantInfos, accessControlList);
					foundObject.setGrants(grantInfos);
					objectName = UUID.randomUUID().toString();
					foundObject.setObjectName(objectName);
					dbObject.add(foundObject);
				} else {
					// object already exists. see if we can modify acl
					if (foundObject.canWriteACP(userId)) {
						List<GrantInfo> grantInfos = foundObject.getGrants();
						foundObject.addGrants(userId, grantInfos, accessControlList);
					}
					objectName = foundObject.getObjectName();
				}
				foundObject.setObjectKey(objectKey);

				try {
					// writes are unconditional
					if (request.getBase64Data().getBytes().length > WalrusProperties.MAX_INLINE_DATA_SIZE) {
						db.rollback();
						throw new InlineDataTooLargeException(bucketName + "/" + objectKey);
					}
					byte[] base64Data = Hashes.base64decode(request.getBase64Data()).getBytes();
					foundObject.setObjectName(objectName);
					FileIO fileIO = null;
					try {
						fileIO = storageManager.prepareForWrite(bucketName, objectName);
						if (fileIO != null) {
							fileIO.write(base64Data);
							fileIO.finish();
						}
					} catch (Exception ex) {
						db.rollback();
						throw new EucalyptusCloudException(ex);
					} finally {
						if (fileIO != null)
							fileIO.finish();
					}
					md5 = Hashes.getHexString(Digest.MD5.get().digest(base64Data));
					foundObject.setEtag(md5);
					Long size = (long) base64Data.length;
					foundObject.setSize(size);
					if (WalrusProperties.shouldEnforceUsageLimits && !request.isAdministrator()) {
						Long bucketSize = bucket.getBucketSize();
						long newSize = bucketSize + oldBucketSize + size;
						if (newSize > (WalrusInfo.getWalrusInfo().getStorageMaxBucketSizeInMB() * WalrusProperties.M)) {
							db.rollback();
							throw new EntityTooLargeException("Key", objectKey, logData);
						}
						bucket.setBucketSize(newSize);
					}
					if (WalrusProperties.trackUsageStatistics) {
						walrusStatistics.updateBytesIn(size);
						walrusStatistics.updateSpaceUsed(size);
					}
					// Add meta data if specified
					if (request.getMetaData() != null)
						foundObject.replaceMetaData(request.getMetaData());

					// TODO: add support for other storage classes
					foundObject.setStorageClass("STANDARD");
					lastModified = new Date();
					foundObject.setLastModified(lastModified);
					if (logData != null) {
						updateLogData(bucket, logData);
						logData.setObjectSize(size);
						reply.setLogData(logData);
					}
				} catch (Exception ex) {
					LOG.error(ex);
					db.rollback();
					throw new EucalyptusCloudException(bucketName);
				}
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}

		db.commit();

		reply.setEtag(md5);
		reply.setLastModified(DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
				+ ".000Z");
		return reply;
	}

	public AddObjectResponseType addObject(AddObjectType request) throws EucalyptusCloudException {

		AddObjectResponseType reply = (AddObjectResponseType) request.getReply();
		String bucketName = request.getBucket();
		String key = request.getKey();
		String userId = request.getUserId();
		String objectName = request.getObjectName();

		AccessControlListType accessControlList = request.getAccessControlList();
		if (accessControlList == null) {
			accessControlList = new AccessControlListType();
		}

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			if (bucket.canWrite(userId)) {
				EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
				ObjectInfo searchObjectInfo = new ObjectInfo();
				searchObjectInfo.setBucketName(bucketName);
				List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
				for (ObjectInfo objectInfo : objectInfos) {
					if (objectInfo.getObjectKey().equals(key)) {
						// key (object) exists.
						db.rollback();
						throw new EucalyptusCloudException("object already exists " + key);
					}
				}
				// write object to bucket
				ObjectInfo objectInfo = new ObjectInfo(bucketName, key);
				objectInfo.setObjectName(objectName);
				List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
				objectInfo.addGrants(userId, grantInfos, accessControlList);
				objectInfo.setGrants(grantInfos);
				dbObject.add(objectInfo);

				objectInfo.setObjectKey(key);
				objectInfo.setOwnerId(userId);
				objectInfo.setSize(storageManager.getSize(bucketName, objectName));
				objectInfo.setEtag(request.getEtag());
				objectInfo.setLastModified(new Date());
				objectInfo.setStorageClass("STANDARD");
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

    public DeleteObjectResponseType deleteObject(DeleteObjectType request) throws EucalyptusCloudException {
        return deleteObject(request, null);
    }

    public DeleteObjectResponseType deleteObject(DeleteObjectType request, SbxRequest r) throws EucalyptusCloudException {
        String bucketName = null;
        String objectKey = null;
        String userId = null;

        DeleteObjectResponseType reply = (DeleteObjectResponseType) request.getReply();
        bucketName = request.getBucket();
        objectKey = request.getKey();
        userId = request.getUserId();

        if (Contexts.SBXSVR) {
            try {
                String myaccount = userId;
                String myinstid = request.getMetaInstId();
                WalrusControl.walrusManager.callWalrusHeartBeat(myaccount, myinstid, "DELETEOBJECT");
            } catch (Throwable t) {
                Log.debug("CSS callWalrusHeartBeat error (deleteObject) " + "***" + t.toString());
            }
        }

        long syncid = -1;
        if (Contexts.SBXSVR) {
            // mac: get sync id before putObject
            try {
                syncid = this.getSyncId(request.getMetaClientType(), userId);
                // syncid must LOCK (syncid == 0 for mobile and browser)
                if (syncid > 0 && r != null && r.reqId != null) {
                    if (SbxDB.syncidlock(syncid) == false) {
                        throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "*** syncid=" + syncid + " need be locked", true);
                    }
                }
            } catch (AccessDeniedException ex) {
                throw ex;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get syncid", true);
            }
        }

        int objSeq = -1;
        if (Contexts.SBXSVR) {
            try {
                if (r != null && StringUtils.isNotEmpty(r.objSeq)) {
                    objSeq = Integer.parseInt(r.objSeq);
                }
                if (objSeq != -1) {
                    // compare objSeq with Object's obj-seq
                    int dbObjSeq = SbxDB.getObjSeq(userId, bucketName, objectKey);
                    if (objSeq != dbObjSeq) {
                        throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "#obj-seq are different: " + objSeq + " != " + dbObjSeq, true);
                    }
                }
            } catch (AccessDeniedException t) {
                throw t;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
            }
        }

        EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
        BucketInfo bucketInfos = new BucketInfo(bucketName);
        String[] backup = new String[1];
        String objectName = null;
        try {
            List<BucketInfo> bucketList = db.query(bucketInfos);
            if (bucketList.size() > 0) {
                BucketInfo bucketInfo = bucketList.get(0);
                BucketLogData logData = bucketInfo.getLoggingEnabled() ? request.getLogData() : null;

                if (bucketInfo.isVersioningEnabled()) {
                    EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
                    ObjectInfo searchDeletedObjectInfo = new ObjectInfo(bucketName, objectKey);
                    searchDeletedObjectInfo.setDeleted(true);
                    try {
                        dbObject.getUnique(searchDeletedObjectInfo);
                        db.rollback();
                        throw new NoSuchEntityException(objectKey, logData);
                    } catch (NoSuchEntityException ex) {
                        throw ex;
                    } catch (EucalyptusCloudException ex) {
                        ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
                        searchObjectInfo.setLast(true);
                        List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
                        for (ObjectInfo objInfo : objectInfos) {
                            objInfo.setLast(false);
                        }
                        ObjectInfo deleteMarker = new ObjectInfo(bucketName, objectKey);
                        deleteMarker.setDeleted(true);
                        deleteMarker.setLast(true);
                        deleteMarker.setOwnerId(userId);
                        deleteMarker.setLastModified(new Date());
                        deleteMarker.setVersionId(UUID.randomUUID().toString().replaceAll("-", ""));
                        dbObject.add(deleteMarker);
                        reply.setCode("200");
                        reply.setDescription("OK");
                    }
                } else {
                    // versioning disabled or suspended.
                    ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
                    searchObjectInfo.setVersionId(WalrusProperties.NULL_VERSION_ID);
                    EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
                    List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
                    if (objectInfos.size() > 0) {
                        ObjectInfo nullObject = objectInfos.get(0);
                        if (nullObject.canWrite(userId)) {
                            if (Contexts.SBXSVR) {
                                //mac: deny delete folder if there are objects inside the folder
                                if (strSBXContentTypeFolder.equals(nullObject.getContentType())) {
                                    if (SbxDB.containObjects (userId, bucketName, objectKey)) {
                                        db.rollback();
                                        throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "folder is not empty (cannot delete)" , true);
                                    }
                                }
                            }

                            dbObject.delete(nullObject);
                            objectName = nullObject.getObjectName();
                            for (GrantInfo grantInfo : nullObject.getGrants()) {
                                db.getEntityManager().remove(grantInfo);
                            }
                            Long size = nullObject.getSize();
                            /*
                             * bucketInfo.setBucketSize(bucketInfo .getBucketSize()
                             * - size);
                             */

                            reply.setCode("200");
                            reply.setDescription("OK");
                            if (logData != null) {
                                updateLogData(bucketInfo, logData);
                                reply.setLogData(logData);
                            }
                            Date lastModified = new Date();
                            if (bucketInfo.isVersioningSuspended()) {
                                // add delete marker
                                ObjectInfo deleteMarker = new ObjectInfo(bucketName, objectKey);
                                deleteMarker.setDeleted(true);
                                deleteMarker.setLast(true);
                                deleteMarker.setOwnerId(userId);
                                deleteMarker.setLastModified(lastModified);
                                deleteMarker.setVersionId(UUID.randomUUID().toString().replaceAll("-", ""));
                                dbObject.add(deleteMarker);
                            }

                            long mtime = 0;
//                            try {
//                                // mac : delete does not need lastModifiedTime
//                                List<MetaDataInfo> metaDataInfos = nullObject.getMetaData();
//                                for (MetaDataInfo metaDataInfo : metaDataInfos) {
//                                    String name = metaDataInfo.getName();
//                                    if (name != null && name.equals("mtime")) {
//                                        mtime = Integer.valueOf(metaDataInfo.getValue());
//                                        break;
//                                    }
//                                }
//                            }catch (Throwable t) {
//                                Log.debug("CSS error: cannot get mtime from client: " + userId+":"+bucketName+":"+objectKey + " ***" + t.toString());
//                            }

                            if (Contexts.SBXSVR) {
                                String reqid = request.getMetaReqId();
                                if (reqid == null) reqid = "null";
                                if (objSeq == -1) {
                                    objSeq = 0;
                                }
                                writeEvent(
                                        dbObject.getEntityManager(),
                                        request.getMetaClientType(),
                                        userId,
                                        "Delete",
                                        nullObject.getObjectKey(),
                                        nullObject.getSize(),
                                        strSBXContentTypeFolder.equals(nullObject.getContentType()),
                                        syncid,
                                        mtime,
                                        reqid, objSeq);
                            }
                            //						ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, objectName, size);
                            //						objectDeleter.start();
                            try {
                                storageManager.deleteObject(backup, bucketName, objectName);
                            } catch (Throwable t) {
                                storageManager.deleteObjectRollback(backup, bucketName, objectName);
                                throw t;
                            }
                            if (WalrusProperties.trackUsageStatistics && (size > 0))
                                walrusStatistics.updateSpaceUsed(-size);
                        } else {
                            db.rollback();
                            throw new AccessDeniedException("Key", objectKey, logData);
                        }
                    } else {
                        db.rollback();
                        throw new NoSuchEntityException(objectKey, logData);
                    }
                }
            } else {
                db.rollback();
                throw new NoSuchBucketException(bucketName);
            }
            try {
                db.commit();
            } catch (Throwable t) {
                storageManager.deleteObjectRollback(backup, bucketName, objectName);
                throw t;
            }
            try {
                storageManager.confirm(backup);
            } catch (Throwable t) {
                //NOP
            }
        }catch(AccessDeniedException t){
            throw t;
        }catch(NoSuchEntityException t){
            throw t;
        }catch(NoSuchBucketException t){
            throw t;
        } catch (Throwable t) {
            db.rollback();
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t, true);
        }
        return reply;
	} //deleteObject

	private class ObjectDeleter extends Thread {
		String bucketName;
		String objectName;
		Long size;

		public ObjectDeleter(String bucketName, String objectName, Long size) {
			this.bucketName = bucketName;
			this.objectName = objectName;
			this.size = size;
		}

		public void run() {
			try {
				storageManager.deleteObject(bucketName, objectName);
				if (WalrusProperties.trackUsageStatistics && (size > 0))
					walrusStatistics.updateSpaceUsed(-size);
			} catch (IOException ex) {
				LOG.error(ex, ex);
			}
		}
	}

	public ListBucketResponseType listBucket(ListBucketType request) throws EucalyptusCloudException {
		ListBucketResponseType reply = (ListBucketResponseType) request.getReply();
		String bucketName = request.getBucket();
		String userId = request.getUserId();
		String prefix = request.getPrefix();
		if (prefix == null)
			prefix = "";

		String marker = request.getMarker();
		int maxKeys = -1;
		String maxKeysString = request.getMaxKeys();
		if (maxKeysString != null) {
            try {
			    maxKeys = Integer.parseInt(maxKeysString);
            } catch (Throwable t) {
                maxKeys = WalrusProperties.MAX_KEYS;
            }
        } else {
			maxKeys = WalrusProperties.MAX_KEYS;
        }
		String delimiter = request.getDelimiter();

        EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
        try {
            BucketInfo bucketInfo = new BucketInfo(bucketName);
            bucketInfo.setHidden(false);
            List<BucketInfo> bucketList = db.query(bucketInfo);

            ArrayList<PrefixEntry> prefixes = new ArrayList<PrefixEntry>();

            if (bucketList.size() > 0) {
                BucketInfo bucket = bucketList.get(0);
                BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
                if (bucket.canRead(userId)) {
                    if (logData != null) {
                        updateLogData(bucket, logData);
                        reply.setLogData(logData);
                    }
                    if (request.isAdministrator()) {
                        EntityWrapper<WalrusSnapshotInfo> dbSnap = db.recast(WalrusSnapshotInfo.class);
                        WalrusSnapshotInfo walrusSnapInfo = new WalrusSnapshotInfo();
                        walrusSnapInfo.setSnapshotBucket(bucketName);
                        List<WalrusSnapshotInfo> walrusSnaps = dbSnap.query(walrusSnapInfo);
                        if (walrusSnaps.size() > 0) {
                            throw new NoSuchBucketException(bucketName);
                        }
                    }
                    reply.setName(bucketName);
                    reply.setIsTruncated(false);
                    if (maxKeys >= 0)
                        reply.setMaxKeys(maxKeys);
                    reply.setPrefix(prefix);
                    reply.setMarker(marker);
                    if (delimiter != null)
                        reply.setDelimiter(delimiter);
                    EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
                    ObjectInfo searchObjectInfo = new ObjectInfo();
                    searchObjectInfo.setBucketName(bucketName);
                    searchObjectInfo.setDeleted(false);
                    searchObjectInfo.setLast(true);
                    List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo, maxKeys, marker,
                            new String[] { "objectKey" });
                    if (objectInfos.size() > 0) {
                        int howManyProcessed = 0;

                        // mac
                        // if (marker != null || objectInfos.size() < maxKeys)
                        // Collections.sort(objectInfos);

                        boolean isFoundMarker = false;
                        ArrayList<ListEntry> contents = new ArrayList<ListEntry>();
                        for (ObjectInfo objectInfo : objectInfos) {
                            String objectKey = objectInfo.getObjectKey();

                            // if (marker != null) {
                            // if (objectKey.compareTo(marker) <= 0)
                            // continue;
                            // }

                            if (marker != null && !isFoundMarker) {
                                if (objectKey.compareTo(marker) == 0) {
                                    isFoundMarker = true;
                                }
                                continue;
                            }

                            if (prefix != null) {
                                if (!objectKey.startsWith(prefix)) {
                                    continue;
                                } else {
                                    if (delimiter != null) {
                                        String[] parts = objectKey.substring(prefix.length()).split(delimiter);
                                        if (parts.length > 1) {
                                            String prefixString = parts[0] + delimiter;
                                            boolean foundPrefix = false;
                                            for (PrefixEntry prefixEntry : prefixes) {
                                                if (prefixEntry.getPrefix().equals(prefixString)) {
                                                    foundPrefix = true;
                                                    break;
                                                }
                                            }
                                            if (!foundPrefix) {
                                                if (maxKeys >= 0) {
                                                    if (howManyProcessed++ > maxKeys) {
                                                        reply.setIsTruncated(true);
                                                        break;
                                                    }
                                                }
                                                prefixes.add(new PrefixEntry(prefixString));
                                            }
                                            continue;
                                        }
                                    }
                                }
                            }
                            if (maxKeys >= 0) {
                                howManyProcessed++;
                                if (howManyProcessed > maxKeys) {
                                    reply.setIsTruncated(true);
                                    break;
                                }
                            }
                            ListEntry listEntry = new ListEntry();
                            listEntry.setKey(objectKey);
                            listEntry.setEtag(objectInfo.getEtag());
                            listEntry.setLastModified(DateUtils.format(objectInfo.getLastModified().getTime(),
                                    DateUtils.ISO8601_DATETIME_PATTERN) + ".000Z");
                            listEntry.setStorageClass(objectInfo.getStorageClass());
                            String displayName = objectInfo.getOwnerId();
                            try {
                                User userInfo = Users.lookupUser(displayName);
                                listEntry.setOwner(new CanonicalUserType(userInfo.getQueryId(), displayName));
                            } catch (NoSuchUserException e) {
                                throw new AccessDeniedException("Bucket", bucketName, logData);
                            }
                            ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
                            objectInfo.returnMetaData(metaData);
                            reply.setMetaData(metaData);

                            // added by keanu 20120709, added obj-seq metadata for each object
                            try {
                                listEntry.setObjSeq(String.valueOf(objectInfo.getObjSeq()));
                            } catch (Throwable t){
                                LOG.debug("cannot add obj-seq");
                            }
                            listEntry.setSize(objectInfo.getSize());
                            listEntry.setStorageClass(objectInfo.getStorageClass());
                            contents.add(listEntry);
                        }
                        reply.setContents(contents);
                        if (prefixes.size() > 0) {
                            reply.setCommonPrefixes(prefixes);
                        }
                    }
                } else {
                    throw new AccessDeniedException("Bucket", bucketName, logData);
                }
            } else {
                throw new NoSuchBucketException(bucketName);
            }
            db.commit();
        } catch (NoSuchBucketException t) {
            db.rollback();
            throw t;
        } catch (AccessDeniedException t) {
            db.rollback();
            throw t;
        } catch (Throwable t) {
            db.rollback();
            throw new AccessDeniedException("CssError", userId + "***" + t.toString(), true);
        }

		return reply;
	}

	public GetObjectAccessControlPolicyResponseType getObjectAccessControlPolicy(
			GetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		GetObjectAccessControlPolicyResponseType reply = (GetObjectAccessControlPolicyResponseType) request
				.getReply();

		String bucketName = request.getBucket();
		String objectKey = request.getKey();
		String userId = request.getUserId();
		String ownerId = null;

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);
		BucketLogData logData;

		AccessControlListType accessControlList = new AccessControlListType();
		if (bucketList.size() > 0) {
			// construct access control policy from grant infos
			BucketInfo bucket = bucketList.get(0);
			logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
			if (bucket.isVersioningEnabled()) {
				if (request.getVersionId() == null)
					searchObjectInfo.setLast(true);
			}
			String versionId = request.getVersionId() != null ? request.getVersionId()
					: WalrusProperties.NULL_VERSION_ID;
			searchObjectInfo.setVersionId(versionId);
			searchObjectInfo.setDeleted(false);
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				ObjectInfo objectInfo = objectInfos.get(0);
				if (objectInfo.canReadACP(userId)) {
					if (logData != null) {
						updateLogData(bucket, logData);
						logData.setObjectSize(objectInfo.getSize());
						reply.setLogData(logData);
					}

					ownerId = objectInfo.getOwnerId();
					ArrayList<Grant> grants = new ArrayList<Grant>();
					List<GrantInfo> grantInfos = objectInfo.getGrants();
					for (GrantInfo grantInfo : grantInfos) {
						String uId = grantInfo.getUserId();
						try {
							User userInfo = Users.lookupUser(uId);
							objectInfo.readPermissions(grants);
							addPermission(grants, userInfo, grantInfo);
						} catch (NoSuchUserException e) {
							throw new AccessDeniedException("Key", objectKey, logData);
						}
					}
					accessControlList.setGrants(grants);
				} else {
					db.rollback();
					throw new AccessDeniedException("Key", objectKey, logData);
				}
			} else {
				db.rollback();
				throw new NoSuchEntityException(objectKey, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}

		AccessControlPolicyType accessControlPolicy = new AccessControlPolicyType();
		try {
			User ownerUserInfo = Users.lookupUser(ownerId);
			accessControlPolicy.setOwner(new CanonicalUserType(ownerUserInfo.getQueryId(), ownerUserInfo
					.getName()));
			accessControlPolicy.setAccessControlList(accessControlList);
		} catch (NoSuchUserException e) {
			throw new AccessDeniedException("Key", objectKey, logData);
		}
		reply.setAccessControlPolicy(accessControlPolicy);
		db.commit();
		return reply;
	}

	public SetBucketAccessControlPolicyResponseType setBucketAccessControlPolicy(
			SetBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		SetBucketAccessControlPolicyResponseType reply = (SetBucketAccessControlPolicyResponseType) request
				.getReply();
		String userId = request.getUserId();
		AccessControlListType accessControlList = request.getAccessControlList();
		String bucketName = request.getBucket();
		if (accessControlList == null) {
			throw new AccessDeniedException("Bucket", bucketName);
		}

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if (bucket.canWriteACP(userId)) {
				List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
				bucket.resetGlobalGrants();
				bucket.addGrants(bucket.getOwnerId(), grantInfos, accessControlList);
				bucket.setGrants(grantInfos);
				reply.setCode("204");
				reply.setDescription("OK");
				if (logData != null) {
					updateLogData(bucket, logData);
					reply.setLogData(logData);
				}
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public SetRESTBucketAccessControlPolicyResponseType setRESTBucketAccessControlPolicy(
			SetRESTBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		SetRESTBucketAccessControlPolicyResponseType reply = (SetRESTBucketAccessControlPolicyResponseType) request
				.getReply();
		String userId = request.getUserId();
		AccessControlPolicyType accessControlPolicy = request.getAccessControlPolicy();
		String bucketName = request.getBucket();
		if (accessControlPolicy == null) {
			throw new AccessDeniedException("Bucket", bucketName);
		}
		AccessControlListType accessControlList = accessControlPolicy.getAccessControlList();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if (bucket.canWriteACP(userId)) {
				List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
				bucket.resetGlobalGrants();
				bucket.addGrants(bucket.getOwnerId(), grantInfos, accessControlList);
				bucket.setGrants(grantInfos);
				reply.setCode("204");
				reply.setDescription("OK");
				if (logData != null) {
					updateLogData(bucket, logData);
					reply.setLogData(logData);
				}
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public SetObjectAccessControlPolicyResponseType setObjectAccessControlPolicy(
			SetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		SetObjectAccessControlPolicyResponseType reply = (SetObjectAccessControlPolicyResponseType) request
				.getReply();
		String userId = request.getUserId();
		AccessControlListType accessControlList = request.getAccessControlList();
		String bucketName = request.getBucket();
		String objectKey = request.getKey();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
			if (bucket.isVersioningEnabled()) {
				if (request.getVersionId() == null)
					searchObjectInfo.setLast(true);
			}
			String versionId = request.getVersionId() != null ? request.getVersionId()
					: WalrusProperties.NULL_VERSION_ID;
			searchObjectInfo.setVersionId(versionId);
			searchObjectInfo.setDeleted(false);
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				ObjectInfo objectInfo = objectInfos.get(0);
				if (!objectInfo.canWriteACP(userId)) {
					db.rollback();
					throw new AccessDeniedException("Key", objectKey, logData);
				}
				List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
				objectInfo.resetGlobalGrants();
				objectInfo.addGrants(objectInfo.getOwnerId(), grantInfos, accessControlList);
				objectInfo.setGrants(grantInfos);

				if (WalrusProperties.enableTorrents) {
					if (!objectInfo.isGlobalRead()) {
						EntityWrapper<TorrentInfo> dbTorrent = db.recast(TorrentInfo.class);
						TorrentInfo torrentInfo = new TorrentInfo(bucketName, objectKey);
						List<TorrentInfo> torrentInfos = dbTorrent.query(torrentInfo);
						if (torrentInfos.size() > 0) {
							TorrentInfo foundTorrentInfo = torrentInfos.get(0);
							TorrentClient torrentClient = Torrents.getClient(bucketName + objectKey);
							if (torrentClient != null) {
								torrentClient.bye();
							}
							dbTorrent.delete(foundTorrentInfo);
						}
					}
				} else {
					LOG.warn("Bittorrent support has been disabled. Please check pre-requisites");
				}
				reply.setCode("204");
				reply.setDescription("OK");
				if (logData != null) {
					updateLogData(bucket, logData);
					logData.setObjectSize(objectInfo.getSize());
					reply.setLogData(logData);
				}
			} else {
				db.rollback();
				throw new NoSuchEntityException(objectKey, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public SetRESTObjectAccessControlPolicyResponseType setRESTObjectAccessControlPolicy(
			SetRESTObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		SetRESTObjectAccessControlPolicyResponseType reply = (SetRESTObjectAccessControlPolicyResponseType) request
				.getReply();
		String userId = request.getUserId();
		AccessControlPolicyType accessControlPolicy = request.getAccessControlPolicy();
		if (accessControlPolicy == null) {
			throw new AccessDeniedException("Key", request.getKey());
		}
		AccessControlListType accessControlList = accessControlPolicy.getAccessControlList();
		String bucketName = request.getBucket();
		String objectKey = request.getKey();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
			if (bucket.isVersioningEnabled()) {
				if (request.getVersionId() == null)
					searchObjectInfo.setLast(true);
			}
			String versionId = request.getVersionId() != null ? request.getVersionId()
					: WalrusProperties.NULL_VERSION_ID;
			searchObjectInfo.setVersionId(versionId);
			searchObjectInfo.setDeleted(false);
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				ObjectInfo objectInfo = objectInfos.get(0);
				if (!objectInfo.canWriteACP(userId)) {
					db.rollback();
					throw new AccessDeniedException("Key", objectKey, logData);
				}
				List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
				objectInfo.resetGlobalGrants();
				objectInfo.addGrants(objectInfo.getOwnerId(), grantInfos, accessControlList);
				objectInfo.setGrants(grantInfos);

				if (WalrusProperties.enableTorrents) {
					if (!objectInfo.isGlobalRead()) {
						EntityWrapper<TorrentInfo> dbTorrent = db.recast(TorrentInfo.class);
						TorrentInfo torrentInfo = new TorrentInfo(bucketName, objectKey);
						List<TorrentInfo> torrentInfos = dbTorrent.query(torrentInfo);
						if (torrentInfos.size() > 0) {
							TorrentInfo foundTorrentInfo = torrentInfos.get(0);
							TorrentClient torrentClient = Torrents.getClient(bucketName + objectKey);
							if (torrentClient != null) {
								torrentClient.bye();
							}
							dbTorrent.delete(foundTorrentInfo);
						}
					}
				} else {
					LOG.warn("Bittorrent support has been disabled. Please check pre-requisites");
				}
				if (logData != null) {
					updateLogData(bucket, logData);
					logData.setObjectSize(objectInfo.getSize());
					reply.setLogData(logData);
				}
				reply.setCode("204");
				reply.setDescription("OK");
			} else {
				db.rollback();
				throw new NoSuchEntityException(objectKey, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

    public GetObjectResponseType getObject(GetObjectType request, SbxRequest req) throws EucalyptusCloudException {
        GetObjectResponseType reply = (GetObjectResponseType) request.getReply();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();
        String userId = request.getUserId();

        if (Contexts.SBXSVR) {
            try {
                String myaccount = userId;
                String myinstid = request.getMetaInstId();
                WalrusControl.walrusManager.callWalrusHeartBeat(myaccount, myinstid, "GETOBJECT");
            } catch (Throwable t) {
                Log.debug("CSS callWalrusHeartBeat error (getObject) " + "***" + t.toString());
            }
        }

        Boolean deleteAfterGet = request.getDeleteAfterGet();
        if (deleteAfterGet == null)
            deleteAfterGet = false;

        Boolean getTorrent = request.getGetTorrent();
        if (getTorrent == null)
            getTorrent = false;

        Boolean getMetaData = request.getGetMetaData();
        if (getMetaData == null)
            getMetaData = false;

        EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
        try {
            BucketInfo bucketInfo = new BucketInfo(bucketName);
            List<BucketInfo> bucketList = db.query(bucketInfo);

            if (bucketList.size() > 0) {
                BucketInfo bucket = bucketList.get(0);
                BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
                boolean versioning = false;
                if (bucket.isVersioningEnabled())
                    versioning = true;
                EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
                ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
                searchObjectInfo.setVersionId(request.getVersionId());
                searchObjectInfo.setDeleted(false);
                if (request.getVersionId() == null)
                    searchObjectInfo.setLast(true);
                List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
                if (objectInfos.size() > 0) {
                    ObjectInfo objectInfo = objectInfos.get(0);
                    if (objectInfo.canRead(userId)) {
                        String objectName = objectInfo.getObjectName();

                        // check if file is exist
                        File file = new File(storageManager.getObjectPath(bucketName, objectName));
                        if (file.exists() == false) {
                            db.rollback();
                            throw new NoSuchEntityException(objectKey, logData);
                        }
                        DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                HttpResponseStatus.OK);
                        if (getMetaData) {
                            List<MetaDataInfo> metaDataInfos = objectInfo.getMetaData();
                            for (MetaDataInfo metaDataInfo : metaDataInfos) {
                                httpResponse.addHeader(
                                        WalrusProperties.AMZ_META_HEADER_PREFIX + metaDataInfo.getName(),
                                        metaDataInfo.getValue());
                            }
                        }

                        // added by keanu 20120709, add obj-seq metadata for each object
                        try {
                            httpResponse.addHeader(WalrusProperties.AMZ_META_HEADER_PREFIX + "obj-seq",	String.valueOf(objectInfo.getObjSeq()));
                        } catch (Throwable t) {
                            LOG.debug("cannot add obj-seq");
                        }

                        if (getTorrent) {
                            if (objectInfo.isGlobalRead()) {
                                if (!WalrusProperties.enableTorrents) {
                                    LOG.warn("Bittorrent support has been disabled. Please check pre-requisites");
                                    throw new AccessDeniedException("Torrents disabled");
                                }
                                EntityWrapper<TorrentInfo> dbTorrent = WalrusControl.getEntityWrapper();
                                TorrentInfo torrentInfo = new TorrentInfo(bucketName, objectKey);
                                TorrentInfo foundTorrentInfo;
                                String absoluteObjectPath = storageManager.getObjectPath(bucketName, objectName);
                                try {
                                    foundTorrentInfo = dbTorrent.getUnique(torrentInfo);
                                } catch (EucalyptusCloudException ex) {
                                    String torrentFile = objectName + ".torrent";
                                    String torrentFilePath = storageManager
                                            .getObjectPath(bucketName, torrentFile);
                                    TorrentCreator torrentCreator = new TorrentCreator(absoluteObjectPath,
                                            objectKey, objectName, torrentFilePath,
                                            WalrusProperties.getTrackerUrl());
                                    try {
                                        torrentCreator.create();
                                    } catch (Exception e) {
                                        LOG.error(e);
                                        throw new AccessDeniedException("could not create torrent file "
                                                + torrentFile);
                                    }
                                    torrentInfo.setTorrentFile(torrentFile);
                                    dbTorrent.add(torrentInfo);
                                    foundTorrentInfo = torrentInfo;
                                }
                                dbTorrent.commit();
                                String torrentFile = foundTorrentInfo.getTorrentFile();
                                String torrentFilePath = storageManager.getObjectPath(bucketName, torrentFile);
                                TorrentClient torrentClient = new TorrentClient(torrentFilePath,
                                        absoluteObjectPath);
                                Torrents.addClient(bucketName + objectKey, torrentClient);
                                torrentClient.start();
                                // send torrent
                                String key = bucketName + "." + objectKey;
                                String randomKey = key + "." + Hashes.getRandom(10);
                                request.setRandomKey(randomKey);

                                File torrent = new File(torrentFilePath);
                                if (torrent.exists()) {
                                    Date lastModified = objectInfo.getLastModified();
                                    db.commit();
                                    long torrentLength = torrent.length();
                                    if (logData != null) {
                                        updateLogData(bucket, logData);
                                        logData.setObjectSize(torrentLength);
                                    }
                                    storageManager.sendObject(
                                            request,
                                            httpResponse,
                                            bucketName,
                                            torrentFile,
                                            torrentLength,
                                            null,
                                            DateUtils.format(lastModified.getTime(),
                                                    DateUtils.ISO8601_DATETIME_PATTERN) + ".000Z",
                                            "application/x-bittorrent", "attachment; filename=" + objectKey
                                            + ".torrent;", request.getIsCompressed(), null, logData);
                                    if (WalrusProperties.trackUsageStatistics) {
                                        walrusStatistics.updateBytesOut(torrentLength);
                                    }
                                    return null;
                                } else {
                                    db.rollback();
                                    String errorString = "Could not get torrent file " + torrentFilePath;
                                    LOG.error(errorString);
                                    throw new AccessDeniedException(errorString);
                                }
                            } else {
                                db.rollback();
                                throw new AccessDeniedException("Key", objectKey, logData);
                            }
                        }
                        Date lastModified = objectInfo.getLastModified();
                        Long size = objectInfo.getSize();
                        if (req != null) {
                            req.content_length = size;
                        }
                        String etag = objectInfo.getEtag();
                        String contentType = objectInfo.getContentType();
                        String contentDisposition = objectInfo.getContentDisposition();
                        db.commit();
                        if (logData != null) {
                            updateLogData(bucket, logData);
                            logData.setObjectSize(size);
                        }
                        String versionId = null;
                        if (versioning) {
                            versionId = objectInfo.getVersionId();
                        }
                        if (request.getGetData()) {
                            if (request.getInlineData()) {
                                if ((size * 4) > WalrusProperties.MAX_INLINE_DATA_SIZE) {
                                    throw new InlineDataTooLargeException(bucketName + "/" + objectKey);
                                }
                                byte[] bytes = new byte[102400];
                                int bytesRead = 0, offset = 0;
                                String base64Data = "";
                                FileIO fileIO = null;
                                try {
                                    fileIO = storageManager.prepareForRead(bucketName, objectName);
                                    while ((bytesRead = fileIO.read(offset)) > 0) {
                                        ByteBuffer buffer = fileIO.getBuffer();
                                        if (buffer != null) {
                                            buffer.get(bytes, 0, bytesRead);
                                            base64Data += new String(bytes, 0, bytesRead);
                                            offset += bytesRead;
                                        }
                                    }
                                    fileIO.finish();
                                } catch (Exception e) {
                                    LOG.error(e, e);
                                    throw new AccessDeniedException(e);
                                } finally {
                                    if (fileIO != null)
                                        fileIO.finish();
                                }
                                reply.setBase64Data(Hashes.base64encode(base64Data));
                            } else {
                                // support for large objects
                                if (WalrusProperties.trackUsageStatistics) {
                                    walrusStatistics.updateBytesOut(objectInfo.getSize());
                                }
                                storageManager.sendObject(
                                        request,
                                        httpResponse,
                                        bucketName,
                                        objectName,
                                        size,
                                        etag,
                                        DateUtils.format(lastModified.getTime(),
                                                DateUtils.ISO8601_DATETIME_PATTERN) + ".000Z", contentType,
                                        contentDisposition, request.getIsCompressed(), versionId, logData);
                                return null;
                            }
                        } else {
                            storageManager.sendHeaders(request, httpResponse, size, etag,
                                    DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
                                            + ".000Z", contentType, contentDisposition, versionId, logData);
                            return null;

                        }
                        reply.setEtag(etag);
                        reply.setLastModified(DateUtils.format(lastModified, DateUtils.ISO8601_DATETIME_PATTERN)
                                + ".000Z");
                        reply.setSize(size);
                        reply.setContentType(contentType);
                        reply.setContentDisposition(contentDisposition);
                        Status status = new Status();
                        status.setCode(200);
                        status.setDescription("OK");
                        reply.setStatus(status);
                        return reply;
                    } else {
                        db.rollback();
                        throw new AccessDeniedException("Key", objectKey, logData);
                    }
                } else {
                    db.rollback();
                    throw new NoSuchEntityException(objectKey, logData);
                }
            } else {
                db.rollback();
                throw new NoSuchBucketException(bucketName);
            }
        } catch (AccessDeniedException t) {
            throw t;
        } catch (NoSuchEntityException t) {
            throw t;
        } catch (NoSuchBucketException t) {
            throw t;
        } catch (Throwable t) {
            db.rollback();
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }
    }

    public GetObjectResponseType getObject(GetObjectType request) throws EucalyptusCloudException {
        return getObject(request, null);
	}

	public GetObjectExtendedResponseType getObjectExtended(GetObjectExtendedType request)
			throws EucalyptusCloudException {
		GetObjectExtendedResponseType reply = (GetObjectExtendedResponseType) request.getReply();
		Long byteRangeStart = request.getByteRangeStart();
		if (byteRangeStart == null) {
			byteRangeStart = 0L;
		}
		Long byteRangeEnd = request.getByteRangeEnd();
		if (byteRangeEnd == null) {
			byteRangeEnd = -1L;
		}
		Date ifModifiedSince = request.getIfModifiedSince();
		Date ifUnmodifiedSince = request.getIfUnmodifiedSince();
		String ifMatch = request.getIfMatch();
		String ifNoneMatch = request.getIfNoneMatch();
		boolean returnCompleteObjectOnFailure = request.getReturnCompleteObjectOnConditionFailure();

		String bucketName = request.getBucket();
		String objectKey = request.getKey();
		String userId = request.getUserId();
		Status status = new Status();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			boolean versioning = false;
			if (bucket.isVersioningEnabled())
				versioning = true;
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				ObjectInfo objectInfo = objectInfos.get(0);

				if (objectInfo.canRead(userId)) {
					String etag = objectInfo.getEtag();
					String objectName = objectInfo.getObjectName();
					if (byteRangeEnd == -1)
						byteRangeEnd = objectInfo.getSize();
					if ((byteRangeStart > objectInfo.getSize()) || (byteRangeStart > byteRangeEnd)
							|| (byteRangeEnd > objectInfo.getSize())
							|| (byteRangeStart < 0 || byteRangeEnd < 0)) {
						throw new InvalidRangeException("Range: " + byteRangeStart + "-" + byteRangeEnd
								+ "object: " + bucketName + "/" + objectKey);
					}
					DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
							HttpResponseStatus.OK);
					if (ifMatch != null) {
						if (!ifMatch.equals(etag) && !returnCompleteObjectOnFailure) {
							db.rollback();
							throw new PreconditionFailedException(objectKey + " etag: " + etag);
						}

					}
					if (ifNoneMatch != null) {
						if (ifNoneMatch.equals(etag) && !returnCompleteObjectOnFailure) {
							db.rollback();
							throw new NotModifiedException(objectKey + " ETag: " + etag);
						}
					}
					Date lastModified = objectInfo.getLastModified();
					if (ifModifiedSince != null) {
						if ((ifModifiedSince.getTime() >= lastModified.getTime())
								&& !returnCompleteObjectOnFailure) {
							db.rollback();
							throw new NotModifiedException(objectKey + " LastModified: "
									+ lastModified.toString());
						}
					}
					if (ifUnmodifiedSince != null) {
						if ((ifUnmodifiedSince.getTime() < lastModified.getTime())
								&& !returnCompleteObjectOnFailure) {
							db.rollback();
							throw new PreconditionFailedException(objectKey + " lastModified: "
									+ lastModified.toString());
						}
					}
					if (request.getGetMetaData()) {
						List<MetaDataInfo> metaDataInfos = objectInfo.getMetaData();
						for (MetaDataInfo metaDataInfo : metaDataInfos) {
							httpResponse.addHeader(
									WalrusProperties.AMZ_META_HEADER_PREFIX + metaDataInfo.getName(),
									metaDataInfo.getValue());
						}
					}
					Long size = objectInfo.getSize();
					String contentType = objectInfo.getContentType();
					String contentDisposition = objectInfo.getContentDisposition();
					db.commit();
					if (logData != null) {
						updateLogData(bucket, logData);
						logData.setObjectSize(size);
					}
					String versionId = null;
					if (versioning) {
						versionId = objectInfo.getVersionId() != null ? objectInfo.getVersionId()
								: WalrusProperties.NULL_VERSION_ID;
					}
					if (request.getGetData()) {
						if (WalrusProperties.trackUsageStatistics) {
							walrusStatistics.updateBytesOut(size);
						}
						storageManager.sendObject(
								request,
								httpResponse,
								bucketName,
								objectName,
								byteRangeStart,
								byteRangeEnd,
								size,
								etag,
								DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN
										+ ".000Z"), contentType, contentDisposition,
								request.getIsCompressed(), versionId, logData);
						return null;
					} else {
						storageManager.sendHeaders(
								request,
								httpResponse,
								size,
								etag,
								DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN
										+ ".000Z"), contentType, contentDisposition, versionId, logData);
						return null;
					}
				} else {
					db.rollback();
					throw new AccessDeniedException("Key", objectKey);
				}
			} else {
				db.rollback();
				throw new NoSuchEntityException(objectKey);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
	}

	public GetBucketLocationResponseType getBucketLocation(GetBucketLocationType request)
			throws EucalyptusCloudException {
		GetBucketLocationResponseType reply = (GetBucketLocationResponseType) request.getReply();
		String bucketName = request.getBucket();
		String userId = request.getUserId();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if (bucket.canRead(userId)) {
				if (logData != null) {
					updateLogData(bucket, logData);
					reply.setLogData(logData);
				}
				String location = bucket.getLocation();
				if (location == null) {
					location = "NotSupported";
				}
				reply.setLocationConstraint(location);
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public CopyObjectResponseType copyObject(CopyObjectType request) throws EucalyptusCloudException {
		CopyObjectResponseType reply = (CopyObjectResponseType) request.getReply();
		String userId = request.getUserId();
		String sourceBucket = request.getSourceBucket();
		String sourceKey = request.getSourceObject();
		String sourceVersionId = request.getSourceVersionId();
		String destinationBucket = request.getDestinationBucket();
		String destinationKey = request.getDestinationObject();
		String metadataDirective = request.getMetadataDirective();
		AccessControlListType accessControlList = request.getAccessControlList();

		String copyIfMatch = request.getCopySourceIfMatch();
		String copyIfNoneMatch = request.getCopySourceIfNoneMatch();
		Date copyIfUnmodifiedSince = request.getCopySourceIfUnmodifiedSince();
		Date copyIfModifiedSince = request.getCopySourceIfModifiedSince();

		if (metadataDirective == null)
			metadataDirective = "COPY";
		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(sourceBucket);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		if (bucketList.size() > 0) {
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(sourceBucket, sourceKey);
			searchObjectInfo.setVersionId(sourceVersionId);
			if (sourceVersionId == null)
				searchObjectInfo.setLast(true);
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				ObjectInfo sourceObjectInfo = objectInfos.get(0);
				if (sourceObjectInfo.canRead(userId)) {
					if (copyIfMatch != null) {
						if (!copyIfMatch.equals(sourceObjectInfo.getEtag())) {
							db.rollback();
							throw new PreconditionFailedException(sourceKey + " CopySourceIfMatch: "
									+ copyIfMatch);
						}
					}
					if (copyIfNoneMatch != null) {
						if (copyIfNoneMatch.equals(sourceObjectInfo.getEtag())) {
							db.rollback();
							throw new PreconditionFailedException(sourceKey + " CopySourceIfNoneMatch: "
									+ copyIfNoneMatch);
						}
					}
					if (copyIfUnmodifiedSince != null) {
						long unmodifiedTime = copyIfUnmodifiedSince.getTime();
						long objectTime = sourceObjectInfo.getLastModified().getTime();
						if (unmodifiedTime < objectTime) {
							db.rollback();
							throw new PreconditionFailedException(sourceKey
									+ " CopySourceIfUnmodifiedSince: " + copyIfUnmodifiedSince.toString());
						}
					}
					if (copyIfModifiedSince != null) {
						long modifiedTime = copyIfModifiedSince.getTime();
						long objectTime = sourceObjectInfo.getLastModified().getTime();
						if (modifiedTime > objectTime) {
							db.rollback();
							throw new PreconditionFailedException(sourceKey + " CopySourceIfModifiedSince: "
									+ copyIfModifiedSince.toString());
						}
					}
					BucketInfo destinationBucketInfo = new BucketInfo(destinationBucket);
					List<BucketInfo> destinationBuckets = db.query(destinationBucketInfo);
					if (destinationBuckets.size() > 0) {
						BucketInfo foundDestinationBucketInfo = destinationBuckets.get(0);
						if (foundDestinationBucketInfo.canWrite(userId)) {
							// all ok
							String destinationVersionId = sourceVersionId;
							ObjectInfo destinationObjectInfo = null;
							String destinationObjectName;
							ObjectInfo destSearchObjectInfo = new ObjectInfo(destinationBucket,
									destinationKey);
							if (foundDestinationBucketInfo.isVersioningEnabled()) {
								if (sourceVersionId != null)
									destinationVersionId = sourceVersionId;
								else
									destinationVersionId = UUID.randomUUID().toString().replaceAll("-", "");
							} else {
								destinationVersionId = WalrusProperties.NULL_VERSION_ID;
							}
							destSearchObjectInfo.setVersionId(destinationVersionId);
							List<ObjectInfo> destinationObjectInfos = dbObject.query(destSearchObjectInfo);
							if (destinationObjectInfos.size() > 0) {
								destinationObjectInfo = destinationObjectInfos.get(0);
								if (!destinationObjectInfo.canWrite(userId)) {
									db.rollback();
									throw new AccessDeniedException("Key", destinationKey);
								}
							}
							boolean addNew = false;
							if (destinationObjectInfo == null) {
								// not found. create a new one
								addNew = true;
								destinationObjectInfo = new ObjectInfo();
								List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
								destinationObjectInfo.setBucketName(destinationBucket);
								destinationObjectInfo.setObjectKey(destinationKey);
								destinationObjectInfo.addGrants(userId, grantInfos, accessControlList);
								destinationObjectInfo.setGrants(grantInfos);
								destinationObjectInfo.setObjectName(UUID.randomUUID().toString());
							} else {
								if (destinationObjectInfo.canWriteACP(userId)) {
									List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
									destinationObjectInfo.addGrants(userId, grantInfos, accessControlList);
									destinationObjectInfo.setGrants(grantInfos);
								}
							}
							destinationObjectInfo.setSize(sourceObjectInfo.getSize());
							destinationObjectInfo.setStorageClass(sourceObjectInfo.getStorageClass());
							destinationObjectInfo.setOwnerId(sourceObjectInfo.getOwnerId());
							destinationObjectInfo.setContentType(sourceObjectInfo.getContentType());
							destinationObjectInfo.setContentDisposition(sourceObjectInfo
									.getContentDisposition());
							String etag = sourceObjectInfo.getEtag();
							Date lastModified = sourceObjectInfo.getLastModified();
							destinationObjectInfo.setEtag(etag);
							destinationObjectInfo.setLastModified(lastModified);
							destinationObjectInfo.setVersionId(destinationVersionId);
							destinationObjectInfo.setLast(true);
							destinationObjectInfo.setDeleted(false);
							if (!metadataDirective.equals("REPLACE")) {
								destinationObjectInfo.setMetaData(sourceObjectInfo.cloneMetaData());
							} else {
								List<MetaDataEntry> metaData = request.getMetaData();
								if (metaData != null)
									destinationObjectInfo.replaceMetaData(metaData);
							}

							String sourceObjectName = sourceObjectInfo.getObjectName();
							destinationObjectName = destinationObjectInfo.getObjectName();

							try {
								storageManager.copyObject(sourceBucket, sourceObjectName, destinationBucket,
										destinationObjectName);
								if (WalrusProperties.trackUsageStatistics)
									walrusStatistics.updateSpaceUsed(sourceObjectInfo.getSize());
							} catch (Exception ex) {
								LOG.error(ex);
								db.rollback();
								throw new EucalyptusCloudException("Could not rename " + sourceObjectName
										+ " to " + destinationObjectName);
							}
							if (addNew)
								dbObject.add(destinationObjectInfo);

							// get rid of delete marker
							ObjectInfo deleteMarker = new ObjectInfo(destinationBucket, destinationKey);
							deleteMarker.setDeleted(true);
							try {
								ObjectInfo foundDeleteMarker = dbObject.getUnique(deleteMarker);
								dbObject.delete(foundDeleteMarker);
							} catch (EucalyptusCloudException ex) {
								// no delete marker found.
								LOG.trace("No delete marker found for: " + destinationBucket + "/"
										+ destinationKey);
							}

							reply.setEtag(etag);
							reply.setLastModified(DateUtils.format(lastModified.getTime(),
									DateUtils.ISO8601_DATETIME_PATTERN) + ".000Z");

							if (foundDestinationBucketInfo.isVersioningEnabled()) {
								reply.setCopySourceVersionId(sourceVersionId);
								reply.setVersionId(destinationVersionId);
							}
							db.commit();
							return reply;
						} else {
							db.rollback();
							throw new AccessDeniedException("Bucket", destinationBucket);
						}
					} else {
						db.rollback();
						throw new NoSuchBucketException(destinationBucket);
					}
				} else {
					db.rollback();
					throw new AccessDeniedException("Key", sourceKey);
				}
			} else {
				db.rollback();
				throw new NoSuchEntityException(sourceKey);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(sourceBucket);
		}
	}

	public SetBucketLoggingStatusResponseType setBucketLoggingStatus(SetBucketLoggingStatusType request)
			throws EucalyptusCloudException {
		SetBucketLoggingStatusResponseType reply = (SetBucketLoggingStatusResponseType) request.getReply();
		String bucket = request.getBucket();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo, targetBucketInfo;
		try {
			bucketInfo = db.getUnique(new BucketInfo(bucket));
		} catch (EucalyptusCloudException ex) {
			db.rollback();
			throw new NoSuchBucketException(bucket);
		}

		if (request.getLoggingEnabled() != null) {
			String targetBucket = request.getLoggingEnabled().getTargetBucket();
			String targetPrefix = request.getLoggingEnabled().getTargetPrefix();
			List<Grant> targetGrantsList = null;
			TargetGrants targetGrants = request.getLoggingEnabled().getTargetGrants();
			if (targetGrants != null)
				targetGrantsList = targetGrants.getGrants();
			if (targetPrefix == null)
				targetPrefix = "";
			try {
				targetBucketInfo = db.getUnique(new BucketInfo(targetBucket));
			} catch (EucalyptusCloudException ex) {
				db.rollback();
				throw new NoSuchBucketException(targetBucket);
			}
			if (!targetBucketInfo.hasLoggingPerms()) {
				db.rollback();
				throw new InvalidTargetBucketForLoggingException(targetBucket);
			}
			bucketInfo.setTargetBucket(targetBucket);
			bucketInfo.setTargetPrefix(targetPrefix);
			bucketInfo.setLoggingEnabled(true);
			if (targetGrantsList != null) {
				targetBucketInfo.addGrants(targetGrantsList);
			}
		} else {
			bucketInfo.setLoggingEnabled(false);
		}
		db.commit();
		return reply;
	}

	public GetBucketLoggingStatusResponseType getBucketLoggingStatus(GetBucketLoggingStatusType request)
			throws EucalyptusCloudException {
		GetBucketLoggingStatusResponseType reply = (GetBucketLoggingStatusResponseType) request.getReply();
		String bucket = request.getBucket();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		try {
			BucketInfo bucketInfo = db.getUnique(new BucketInfo(bucket));
			if (bucketInfo.getLoggingEnabled()) {
				String targetBucket = bucketInfo.getTargetBucket();
				ArrayList<Grant> grants = new ArrayList<Grant>();
				try {
					BucketInfo targetBucketInfo = db.getUnique(new BucketInfo(targetBucket));
					List<GrantInfo> grantInfos = targetBucketInfo.getGrants();
					for (GrantInfo grantInfo : grantInfos) {
						String uId = grantInfo.getUserId();
						try {
							if (uId != null) {
								User grantUserInfo = Users.lookupUser(uId);
								addPermission(grants, grantUserInfo, grantInfo);
							} else {
								addPermission(grants, grantInfo);
							}
						} catch (NoSuchUserException e) {
							db.rollback();
							throw new AccessDeniedException("Bucket", targetBucket);
						}
					}
				} catch (EucalyptusCloudException ex) {
					db.rollback();
					throw new InvalidTargetBucketForLoggingException(targetBucket);
				}
				LoggingEnabled loggingEnabled = new LoggingEnabled();
				loggingEnabled.setTargetBucket(bucketInfo.getTargetBucket());
				loggingEnabled.setTargetPrefix(bucketInfo.getTargetPrefix());

				TargetGrants targetGrants = new TargetGrants();
				targetGrants.setGrants(grants);
				loggingEnabled.setTargetGrants(targetGrants);
				reply.setLoggingEnabled(loggingEnabled);
			}
		} catch (EucalyptusCloudException ex) {
			db.rollback();
			throw new NoSuchBucketException(bucket);
		}
		db.commit();
		return reply;
	}

	private void updateLogData(BucketInfo bucket, BucketLogData logData) {
		logData.setOwnerId(bucket.getOwnerId());
		logData.setTargetBucket(bucket.getTargetBucket());
		logData.setTargetPrefix(bucket.getTargetPrefix());
	}

	public GetBucketVersioningStatusResponseType getBucketVersioningStatus(
			GetBucketVersioningStatusType request) throws EucalyptusCloudException {
		GetBucketVersioningStatusResponseType reply = (GetBucketVersioningStatusResponseType) request
				.getReply();
		String bucket = request.getBucket();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		try {
			BucketInfo bucketInfo = db.getUnique(new BucketInfo(bucket));
			if (bucketInfo.getVersioning() != null) {
				String status = bucketInfo.getVersioning();
				if (WalrusProperties.VersioningStatus.Disabled.toString().equals(status))
					reply.setVersioningStatus(WalrusProperties.VersioningStatus.Suspended.toString());
				else
					reply.setVersioningStatus(status);
			}
		} catch (EucalyptusCloudException ex) {
			db.rollback();
			throw new NoSuchBucketException(bucket);
		}
		db.commit();
		return reply;
	}

	public SetBucketVersioningStatusResponseType setBucketVersioningStatus(
			SetBucketVersioningStatusType request) throws EucalyptusCloudException {
		SetBucketVersioningStatusResponseType reply = (SetBucketVersioningStatusResponseType) request
				.getReply();
		String bucket = request.getBucket();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo;
		try {
			bucketInfo = db.getUnique(new BucketInfo(bucket));
		} catch (EucalyptusCloudException ex) {
			db.rollback();
			throw new NoSuchBucketException(bucket);
		}

		if (request.getVersioningStatus() != null) {
			String status = request.getVersioningStatus();
			if (WalrusProperties.VersioningStatus.Enabled.toString().equals(status))
				bucketInfo.setVersioning(WalrusProperties.VersioningStatus.Enabled.toString());
			else if (WalrusProperties.VersioningStatus.Suspended.toString().equals(status)
					&& WalrusProperties.VersioningStatus.Enabled.toString()
							.equals(bucketInfo.getVersioning()))
				bucketInfo.setVersioning(WalrusProperties.VersioningStatus.Suspended.toString());
		}
		db.commit();
		return reply;
	}

	public ListVersionsResponseType listVersions(ListVersionsType request) throws EucalyptusCloudException {
		ListVersionsResponseType reply = (ListVersionsResponseType) request.getReply();
		String bucketName = request.getBucket();
		String userId = request.getUserId();
		String prefix = request.getPrefix();
		if (prefix == null)
			prefix = "";

		String keyMarker = request.getKeyMarker();
		String versionIdMarker = request.getVersionIdMarker();

		int maxKeys = -1;
		String maxKeysString = request.getMaxKeys();
		if (maxKeysString != null)
			maxKeys = Integer.parseInt(maxKeysString);
		else
			maxKeys = WalrusProperties.MAX_KEYS;

		String delimiter = request.getDelimiter();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfo = new BucketInfo(bucketName);
		bucketInfo.setHidden(false);
		List<BucketInfo> bucketList = db.query(bucketInfo);

		ArrayList<PrefixEntry> prefixes = new ArrayList<PrefixEntry>();

		if (bucketList.size() > 0) {
			BucketInfo bucket = bucketList.get(0);
			BucketLogData logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
			if (bucket.canRead(userId)) {
				if (bucket.isVersioningDisabled()) {
					db.rollback();
					throw new EucalyptusCloudException("Versioning has not been enabled for bucket: "
							+ bucketName);
				}
				if (logData != null) {
					updateLogData(bucket, logData);
					reply.setLogData(logData);
				}
				if (request.isAdministrator()) {
					EntityWrapper<WalrusSnapshotInfo> dbSnap = db.recast(WalrusSnapshotInfo.class);
					WalrusSnapshotInfo walrusSnapInfo = new WalrusSnapshotInfo();
					walrusSnapInfo.setSnapshotBucket(bucketName);
					List<WalrusSnapshotInfo> walrusSnaps = dbSnap.query(walrusSnapInfo);
					if (walrusSnaps.size() > 0) {
						db.rollback();
						throw new NoSuchBucketException(bucketName);
					}
				}
				reply.setName(bucketName);
				reply.setIsTruncated(false);
				if (maxKeys >= 0)
					reply.setMaxKeys(maxKeys);
				reply.setPrefix(prefix);
				reply.setKeyMarker(keyMarker);
				reply.setVersionIdMarker(versionIdMarker);
				if (delimiter != null)
					reply.setDelimiter(delimiter);
				EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
				ObjectInfo searchObjectInfo = new ObjectInfo();
				searchObjectInfo.setBucketName(bucketName);
				List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
				if (objectInfos.size() > 0) {
					int howManyProcessed = 0;
					if (keyMarker != null || objectInfos.size() < maxKeys)
						Collections.sort(objectInfos);
					ArrayList<VersionEntry> versions = new ArrayList<VersionEntry>();
					ArrayList<DeleteMarkerEntry> deleteMarkers = new ArrayList<DeleteMarkerEntry>();

					for (ObjectInfo objectInfo : objectInfos) {
						String objectKey = objectInfo.getObjectKey();

						if (keyMarker != null) {
							if (objectKey.compareTo(keyMarker) <= 0)
								continue;
						} else if (versionIdMarker != null) {
							if (!objectInfo.getVersionId().equals(versionIdMarker))
								continue;
						}

						if (prefix != null) {
							if (!objectKey.startsWith(prefix)) {
								continue;
							} else {
								if (delimiter != null) {
									String[] parts = objectKey.substring(prefix.length()).split(delimiter);
									if (parts.length > 1) {
										String prefixString = parts[0] + delimiter;
										boolean foundPrefix = false;
										for (PrefixEntry prefixEntry : prefixes) {
											if (prefixEntry.getPrefix().equals(prefixString)) {
												foundPrefix = true;
												break;
											}
										}
										if (!foundPrefix) {
											prefixes.add(new PrefixEntry(prefixString));
											if (maxKeys >= 0) {
												if (howManyProcessed++ >= maxKeys) {
													reply.setIsTruncated(true);
													break;
												}
											}
										}
										continue;
									}
								}
							}
						}
						if (maxKeys >= 0) {
							if (howManyProcessed++ >= maxKeys) {
								reply.setIsTruncated(true);
								break;
							}
						}
						if (!objectInfo.getDeleted()) {
							VersionEntry versionEntry = new VersionEntry();
							versionEntry.setKey(objectKey);
							versionEntry.setVersionId(objectInfo.getVersionId());
							versionEntry.setEtag(objectInfo.getEtag());
							versionEntry.setLastModified(DateUtils.format(objectInfo.getLastModified()
									.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
									+ ".000Z");
							String displayName = objectInfo.getOwnerId();
							try {
								User userInfo = Users.lookupUser(displayName);
								versionEntry.setOwner(new CanonicalUserType(userInfo.getQueryId(),
										displayName));
							} catch (NoSuchUserException e) {
								db.rollback();
								throw new AccessDeniedException("Bucket", bucketName, logData);
							}
							versionEntry.setSize(objectInfo.getSize());
							versionEntry.setStorageClass(objectInfo.getStorageClass());
							versionEntry.setIsLatest(objectInfo.getLast());
							versions.add(versionEntry);
						} else {
							DeleteMarkerEntry deleteMarkerEntry = new DeleteMarkerEntry();
							deleteMarkerEntry.setKey(objectKey);
							deleteMarkerEntry.setVersionId(objectInfo.getVersionId());
							deleteMarkerEntry.setLastModified(DateUtils.format(objectInfo.getLastModified()
									.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
									+ ".000Z");
							String displayName = objectInfo.getOwnerId();
							try {
								User userInfo = Users.lookupUser(displayName);
								deleteMarkerEntry.setOwner(new CanonicalUserType(userInfo.getQueryId(),
										displayName));
							} catch (NoSuchUserException e) {
								db.rollback();
								throw new AccessDeniedException("Bucket", bucketName, logData);
							}
							deleteMarkerEntry.setIsLatest(objectInfo.getLast());
							deleteMarkers.add(deleteMarkerEntry);
						}
					}
					reply.setVersions(versions);
					reply.setDeleteMarkers(deleteMarkers);
					if (prefix != null) {
						reply.setCommonPrefixes(prefixes);
					}
				}
			} else {
				db.rollback();
				throw new AccessDeniedException("Bucket", bucketName, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

	public DeleteVersionResponseType deleteVersion(DeleteVersionType request) throws EucalyptusCloudException {
		DeleteVersionResponseType reply = (DeleteVersionResponseType) request.getReply();
		String bucketName = request.getBucket();
		String objectKey = request.getKey();
		String userId = request.getUserId();

		EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
		BucketInfo bucketInfos = new BucketInfo(bucketName);
		List<BucketInfo> bucketList = db.query(bucketInfos);

		if (bucketList.size() > 0) {
			BucketInfo bucketInfo = bucketList.get(0);
			BucketLogData logData = bucketInfo.getLoggingEnabled() ? request.getLogData() : null;
			ObjectInfo foundObject = null;
			EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
			ObjectInfo searchObjectInfo = new ObjectInfo(bucketName, objectKey);
			if (request.getVersionid() == null) {
				db.rollback();
				throw new EucalyptusCloudException("versionId is null");
			}
			searchObjectInfo.setVersionId(request.getVersionid());
			List<ObjectInfo> objectInfos = dbObject.query(searchObjectInfo);
			if (objectInfos.size() > 0) {
				foundObject = objectInfos.get(0);
			}

			if (foundObject != null) {
				if (foundObject.canWrite(userId)) {
					dbObject.delete(foundObject);
					if (!foundObject.getDeleted()) {
						String objectName = foundObject.getObjectName();
						for (GrantInfo grantInfo : foundObject.getGrants()) {
							db.getEntityManager().remove(grantInfo);
						}
						Long size = foundObject.getSize();
						bucketInfo.setBucketSize(bucketInfo.getBucketSize() - size);
						ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, objectName, size);
						objectDeleter.start();
					}
					reply.setCode("200");
					reply.setDescription("OK");
					if (logData != null) {
						updateLogData(bucketInfo, logData);
						reply.setLogData(logData);
					}
				} else {
					db.rollback();
					throw new AccessDeniedException("Key", objectKey, logData);
				}
			} else {
				db.rollback();
				throw new NoSuchEntityException(objectKey, logData);
			}
		} else {
			db.rollback();
			throw new NoSuchBucketException(bucketName);
		}
		db.commit();
		return reply;
	}

    // Refactorred PutObject
    public PutObjectResponseType putObjectRefactor(PutObjectType request) throws EucalyptusCloudException {
        //call renameObject
        if(request.getRenameto()!=null){return this.renameObject(request, null);}

        //do putObject
        PutObjectResponseType reply = (PutObjectResponseType) request.getReply();
        String userId = request.getUserId();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();

        //get channel
        Channel channel = null;
        try {
            channel = Contexts.lookup(request.getCorrelationId()).getChannel();
        } catch (Throwable t) {
            Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error get channel" + "***" + t.toString());
            throw new AccessDeniedException("GetChannelError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get channel", true);
        }

        long syncid = -1;
        long mtime = 0;
        if (Contexts.SBXSVR) {
            //get sync id before putObject
            try {
                syncid = this.getSyncId(request.getMetaClientType(), userId);
                // syncid must LOCK
                //...
            } catch (AccessDeniedException ex) {
                Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error get syncid" + "***" + ex.toString());
                throw ex;
            } catch (Throwable t) {
                Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error get syncid" + "***" + t.toString());
                throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get syncid", true);
            }

            //get mtime before putObject
            try {
                String strModifiedTime = request.getMetaModifiedTime();
                if (strModifiedTime != null && strModifiedTime.isEmpty()==false) {
                    mtime = Long.valueOf(strModifiedTime);
                }
            }catch (Throwable t) {
                Log.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Error get mtime" + "***" + t.toString());
                throw new AccessDeniedException("GetMtimeError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get mtime", true);
            }
        }


        int objSeq = -1;
        if (Contexts.SBXSVR) {
            try {
                String rObjSeq = request.getMetaObjSeq();
                if (StringUtils.isNotEmpty(rObjSeq)) {
                    objSeq = Integer.parseInt(rObjSeq);
                }
                if (objSeq != -1) {
                    // compare objSeq with Object's obj-seq
                    int dbObjSeq = SbxDB.getObjSeq(userId, bucketName, objectKey);
                    if (dbObjSeq != -1 && objSeq != dbObjSeq) {
                        throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "#obj-seq are different: " + objSeq + " != " + dbObjSeq, true);
                    }
                }
            } catch (AccessDeniedException t) {
                throw t;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////
        // check bucket and object exist and writable
        ////////////////////////////////////////////////////////////////////////////////////
        Long oldBucketSize = 0L;

        String md5 = "";
        Date lastModified = null;

        AccessControlListType accessControlList = request.getAccessControlList();
        if (accessControlList == null) {
            accessControlList = new AccessControlListType();
        }
        String key = bucketName + "." + objectKey;
        String randomKey = request.getRandomKey();
        WalrusDataMessenger messenger = WalrusRESTBinding.getWriteMessenger();

        EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
        String objectName;
        String versionId = WalrusProperties.NULL_VERSION_ID;
        boolean bAddObject = false;
        BucketLogData logData = null;
        BucketInfo bucket = null;
        try {
            // check if bucket is exist
            BucketInfo bucketInfo = new BucketInfo(bucketName);
            List<BucketInfo> bucketList = db.query(bucketInfo);
            if (bucketList.size() <= 0) {
                db.rollback();
                messenger.removeQueue(key, randomKey);
                throw new NoSuchBucketException(bucketName);
            }

            // check if bucket is writable
            bucket = bucketList.get(0);
            logData = bucket.getLoggingEnabled() ? request.getLogData() : null;
            if (!bucket.canWrite(userId)) {
                db.rollback();
                messenger.removeQueue(key, randomKey);
                throw new AccessDeniedException("Bucket", bucketName, logData);
            }

            // Version is denied
            if (bucket.isVersioningEnabled()) {
                db.rollback();
                messenger.removeQueue(key, randomKey);
                throw new AccessDeniedException("Version is denied", bucketName, logData);
            }

            if (logData != null)
                reply.setLogData(logData);

            EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
            try {
                // To update object
                bAddObject = false;
                ObjectInfo searchObject = new ObjectInfo(bucketName, objectKey);
                searchObject.setVersionId(versionId);
                ObjectInfo foundObject = dbObject.getUnique(searchObject);
                // check if object is writable
                if (!foundObject.canWrite(userId)) {
                    db.rollback();
                    messenger.removeQueue(key, randomKey);
                    throw new AccessDeniedException("Key", objectKey, logData);
                }
                objectName = foundObject.getObjectName();
            } catch (EucalyptusCloudException ex) {
                // To insert object
                bAddObject = true;
                objectName = UUID.randomUUID().toString();
            }

            db.commit();
        } catch (NoSuchBucketException ex) {
            throw ex;
        } catch (AccessDeniedException ex) {
            throw ex;
        } catch (Throwable t) {
            db.rollback();
            messenger.removeQueue(key, randomKey);
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }

        ////////////////////////////////////////////////////////////////////////////////////
        // receive payload
        ////////////////////////////////////////////////////////////////////////////////////
        // writes are unconditional
        WalrusDataQueue<WalrusDataMessage> putQueue = messenger.getQueue(key, randomKey);
        String tempObjectName = UUID.randomUUID().toString() + ".tmp";
        FileIO fileIO = null;
        MessageDigest digest = null;
        long size = 0;
        WalrusDataMessage dataMessage = null;
        try {
            boolean isTimeout = true;
            while ((isTimeout = true) && (dataMessage = putQueue.poll(SbxPutRequest.DATAQUEUE_TIMEOUT, TimeUnit.SECONDS)) != null) {
                isTimeout = false;
                // duplicate put object at a same time
                if (putQueue.getInterrupted()) {
                    if (WalrusDataMessage.isEOF(dataMessage) || WalrusDataMessage.isDisconnected(dataMessage)) {
                        break;
                    }
                    continue;
                }
                if (WalrusDataMessage.isStart(dataMessage)) {
                    digest = Digest.MD5.get();
                    try {
                        fileIO = storageManager.prepareForWrite(bucketName, tempObjectName);
                    }finally {
                        // To garbage collection
                        addPutObjectCallback(null, channel, fileIO, messenger, key, randomKey, putQueue, userId, bucketName, objectKey, tempObjectName);
                    }
                } else if (WalrusDataMessage.isEOF(dataMessage)) {
                    break;
                } else if (WalrusDataMessage.isDisconnected(dataMessage)) {
                    LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Disconnected Error");
                    messenger.removeQueue(key, randomKey);
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Disconnected Error", true);
                } else {
                    assert (WalrusDataMessage.isData(dataMessage));
                    byte[] data = dataMessage.getPayload();
                    // start writing object (but do not commit yet)
                    try {
                        if (fileIO != null)
                            fileIO.write(data);
                    } catch (IOException ex) {
                        LOG.error(ex);
                    }
                    // calculate md5 on the fly
                    size += data.length;
                    if (digest != null)
                        digest.update(data);
                }
            }// while
            // pubObject is interrupted (and/or timeout)
            if (putQueue.getInterrupted()) {
                LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "Duplicate putObject Error");
                if (fileIO != null) fileIO.finish();
                ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, tempObjectName, -1L);
                objectDeleter.start();
                messenger.removeQueue(key, randomKey);
                throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Duplicate putObject Error", true);
            }
            // pubObject is timeout
            if (isTimeout) {
                LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + "DataQueue Timeout Error");
                if (fileIO != null) fileIO.finish();
                ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, tempObjectName, -1L);
                objectDeleter.start();
                messenger.removeQueue(key, randomKey);
                throw new CSSException(userId + ":" + bucketName + ":" + objectKey + "***" + "DataQueue Timeout Error");
            }
        } catch (AccessDeniedException ex) {
            throw ex;
        } catch (Throwable t) {
            LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + t.toString());
            messenger.removeQueue(key, randomKey);
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        } finally {
            if (fileIO != null) fileIO.finish();
        }

        ////////////////////////////////////////////////////////////////////////////////////
        // Insert or Update object
        ////////////////////////////////////////////////////////////////////////////////////
        EntityWrapper<ObjectInfo> dbObject = WalrusControl.getEntityWrapper();
        try {
            if (digest != null)
                md5 = Hashes.bytesToHex(digest.digest());
            lastModified = new Date();
            ObjectInfo searchObject = new ObjectInfo(bucketName, objectKey);
            searchObject.setVersionId(versionId);
            List<ObjectInfo> objectInfos = dbObject.query(new ObjectInfo(bucketName,
                    objectKey));
            for (ObjectInfo objInfo : objectInfos) {
                objInfo.setLast(false);
            }
            ObjectInfo foundObject = null;
            ObjectInfo objectInfo = null;
            try {
                // bbAddObject should be false
                foundObject = dbObject.getUnique(searchObject);
                if (foundObject.canWriteACP(userId)) {
                    List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
                    foundObject.addGrants(userId, grantInfos, accessControlList);
                    foundObject.setGrants(grantInfos);
                }
            } catch (EucalyptusCloudException ex) {
                // bbAddObject should be true
                if (bAddObject == false) {
                    // After received payload, the original object is missing (deleted or renamed)
                    dbObject.rollback();
                    ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, tempObjectName, -1L);
                    objectDeleter.start();
                    messenger.removeQueue(key, randomKey);
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Unable to update object", true);
                }

                objectInfo = new ObjectInfo(bucketName, objectKey);
                objectInfo.setOwnerId(userId);
                List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
                objectInfo.addGrants(userId, grantInfos, accessControlList);
                objectInfo.setGrants(grantInfos);
                objectInfo.setObjectName(objectName);
                objectInfo.setSize(0L);
                foundObject = objectInfo;
            }

            if (objSeq == -1) {
                objSeq = 0;
            } else {
                objSeq++;
            }

            foundObject.setVersionId(versionId);
            foundObject.replaceMetaData(request.getMetaData());
            foundObject.setEtag(md5);
            foundObject.setSize(size);
            foundObject.setLastModified(lastModified);
            foundObject.setStorageClass("STANDARD");
            foundObject.setContentType(request.getContentType());
            foundObject.setContentDisposition(request.getContentDisposition());
            foundObject.setLast(true);
            foundObject.setDeleted(false);
            foundObject.setObjSeq(objSeq);
            reply.setSize(size);

            String op = null;
            if (bAddObject) {
                dbObject.add(foundObject);
                op = "Create";
            } else {
                op = "Create";
                //mac: to change op to modify
                // op = "Modify";
            }

            if (Contexts.SBXSVR) {
                String reqid = request.getMetaReqId();
                if (reqid == null) reqid = "null";
                //LOG.debug("CSS writeEvent " + op + "): " + userId+":"+bucketName+":"+objectKey);
                this.writeEvent(
                        dbObject.getEntityManager(),
                        request.getMetaClientType(),
                        userId,
                        op,
                        foundObject.getObjectKey(),
                        foundObject.getSize(),
                        strSBXContentTypeFolder.equals(foundObject.getContentType()),
                        syncid,
                        mtime,
                        reqid, objSeq);
            }

            storageManager.renameObject(bucketName, tempObjectName, objectName);

            dbObject.commit();

            try {
                if (bucket.isVersioningEnabled()) {
                    reply.setVersionId(versionId);
                }
                if (WalrusProperties.trackUsageStatistics) {
                    walrusStatistics.updateBytesIn(size);
                    walrusStatistics.updateSpaceUsed(size);
                }
                if (logData != null) {
                    logData.setObjectSize(size);
                    updateLogData(bucket, logData);
                    logData.setTurnAroundTime(Long.parseLong(new String(dataMessage.getPayload())));
                }
            } catch (Throwable t) {
                LOG.debug("Error Update Info: " + key);
            }

            // restart all interrupted puts
            WalrusMonitor monitor = messenger.getMonitor(key);
            synchronized (monitor) {
                monitor.setLastModified(lastModified);
                monitor.setMd5(md5);
                monitor.notifyAll();
            }
            messenger.removeMonitor(key);
            messenger.removeQueue(key, randomKey);
            LOG.info("Transfer complete: " + key);
        } catch (Throwable t) {
            LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + t.toString());
            dbObject.rollback();
            messenger.removeQueue(key, randomKey);
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }

        try {
            reply.setEtag(md5);
            reply.setLastModified(DateUtils.format(lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
                    + ".000Z");
        } catch (Throwable t) {
            LOG.debug("Error Set Put Reply: " + key);
        }

        return reply;
    }//refactored putObject

    public void putObjectStage0(PutObjectType request, SbxPutRequest req) throws EucalyptusCloudException {
        /////////////////////////////////////////////////
        // get channel and check Safebox variables
        /////////////////////////////////////////////////
        req.reply = request.getReply();
        String userId = request.getUserId();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();

        //get channel
        try {
            if (req.channel == null && req.channelctx != null) {
                req.channel = req.channelctx.getChannel();
            }
            if (req.channel == null) {
                req.channel = Contexts.getChannel(req);
            }
        } catch (Throwable t) {
            throw new AccessDeniedException("GetChannelError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get channel", true);
        }

        if (Contexts.SBXSVR) {
            try {
                String myaccount = userId;
                String myinstid = request.getMetaInstId();
                WalrusControl.walrusManager.callWalrusHeartBeat(myaccount, myinstid, "PUTOBJECT");
            } catch (Throwable t) {
                Log.debug("CSS callWalrusHeartBeat error (putObject) " + "***" + t.toString());
            }
        }

        if (Contexts.SBXSVR) {
            //get sync id before putObject
            try {
                req.syncid = this.getSyncId(request.getMetaClientType(), userId);
                // syncid must LOCK (syncid == 0 for mobile and browser)
                if (req.syncid > 0 && req.reqId != null) {
                    if (SbxDB.syncidlock(req.syncid) == false) {
                        throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "*** syncid=" + req.syncid + " need be locked", true);
                    }
                }
            } catch (AccessDeniedException ex) {
                throw ex;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get syncid", true);
            }

            //get mtime before putObject
            try {
                String strModifiedTime = request.getMetaModifiedTime();
                if (strModifiedTime != null && strModifiedTime.isEmpty()==false) {
                    req.mtime = Long.valueOf(strModifiedTime);
                }
            }catch (Throwable t) {
                throw new AccessDeniedException("GetMtimeError", userId + ":" + bucketName + ":" + objectKey + "***" + "Error get mtime", true);
            }
        }

        int objSeq = -1;
        if (Contexts.SBXSVR) {
            try {
                if (StringUtils.isNotEmpty(req.objSeq)) {
                    objSeq = Integer.parseInt(req.objSeq);
                }
                if (objSeq != -1) {
                    // compare objSeq with Object's obj-seq
                    int dbObjSeq = SbxDB.getObjSeq(userId, bucketName, objectKey);
                    if (dbObjSeq != -1 && objSeq != dbObjSeq) {
                        throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "#obj-seq are different: " + objSeq + " != " + dbObjSeq, true);
                    }
                }
            } catch (AccessDeniedException t) {
                throw t;
            } catch (Throwable t) {
                throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
            }
        }

        /////////////////////////////////////////////////
        req.reqStage = SbxPutRequest.STAGE.S0;
        /////////////////////////////////////////////////
    }//putObjectStage0

    public void putObjectStage1(PutObjectType request, SbxPutRequest req) throws EucalyptusCloudException {
        /////////////////////////////////////////////////
        // check bucket and object permissions
        /////////////////////////////////////////////////
        String userId = request.getUserId();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();

        Long oldBucketSize = 0L;

        req.accessControlList = request.getAccessControlList();
        if (req.accessControlList == null) {
            req.accessControlList = new AccessControlListType();
        }
        req.key = bucketName + "." + objectKey;
        req.randomKey = request.getRandomKey();
        req.messenger = WalrusRESTBinding.getWriteMessenger();

        EntityWrapper<BucketInfo> db = WalrusControl.getEntityWrapper();
        try {
            // check if bucket is exist
            BucketInfo bucketInfo = new BucketInfo(bucketName);
            List<BucketInfo> bucketList = db.query(bucketInfo);
            if (bucketList.size() <= 0) {
                db.rollback();
                req.messenger.removeQueue(req.key, req.randomKey);
                throw new NoSuchBucketException(bucketName);
            }

            // check if bucket is writable
            req.bucket = bucketList.get(0);
            req.logData = req.bucket.getLoggingEnabled() ? request.getLogData() : null;
            if (!req.bucket.canWrite(userId)) {
                db.rollback();
                req.messenger.removeQueue(req.key, req.randomKey);
                throw new AccessDeniedException("Bucket", bucketName, req.logData);
            }

            // Version is denied
            if (req.bucket.isVersioningEnabled()) {
                db.rollback();
                req.messenger.removeQueue(req.key, req.randomKey);
                throw new AccessDeniedException("VersionError", "Version is not support yet" + bucketName, true);
            }

            if (req.logData != null)
                req.reply.setLogData(req.logData);

            EntityWrapper<ObjectInfo> dbObject = db.recast(ObjectInfo.class);
            try {
                // To update object
                req.bAddObject = false;
                ObjectInfo searchObject = new ObjectInfo(bucketName, objectKey);
                searchObject.setVersionId(req.versionId);
                ObjectInfo foundObject = dbObject.getUnique(searchObject);
                // check if object is writable
                if (!foundObject.canWrite(userId)) {
                    db.rollback();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    throw new AccessDeniedException("Key", objectKey, req.logData);
                }
                req.objectName = foundObject.getObjectName();
            } catch (EucalyptusCloudException ex) {
                // To insert object
                req.bAddObject = true;
                req.objectName = UUID.randomUUID().toString();
            }

            db.commit();
        } catch (NoSuchBucketException ex) {
            throw ex;
        } catch (AccessDeniedException ex) {
            throw ex;
        } catch (Throwable t) {
            db.rollback();
            req.messenger.removeQueue(req.key, req.randomKey);
            throw new AccessDeniedException("CSSError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }

        /////////////////////////////////////////////////
        req.reqStage = SbxPutRequest.STAGE.S1;
        /////////////////////////////////////////////////
    }//putObjectStage1

    private void addPutObjectCallback(final SbxPutRequest req,
                                      final Channel channel,
                                      final FileIO myFileIO,
                                      final WalrusDataMessenger myMessenger,
                                      final String myKey,
                                      final String myRandomKey,
                                      final WalrusDataQueue<WalrusDataMessage> myPutQueue,
                                      final String myUserId,
                                      final String myBucketName,
                                      final String myObjectKey,
                                      final String myTempObjectName) {
        try {
            ChannelFuture closeFuture = channel.getCloseFuture();
            closeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    try {
                        if (req != null) {
                            req.disconnect = true;
                        }
                    } catch (Throwable t) {
                        //NOP
                    }
                    try {
                        myFileIO.finish();
                    } catch (Throwable t) {
                        //NOP
                    }
                    try {
                        myMessenger.removeMonitor(myKey);
                    } catch (Throwable t) {
                        //NOP
                    }
                    try {
                        myMessenger.removeQueue(myKey, myRandomKey);
                    } catch (Throwable t) {
                        //NOP
                    }
                    // Delete orphaned files
                    try {
                        File file = new File(storageManager.getObjectPath(myBucketName, myTempObjectName));
                        if (file.exists() == true) {
                            ObjectDeleter objectDeleter = new ObjectDeleter(myBucketName, myTempObjectName, -1L);
                            objectDeleter.start();
                        }
                    } catch (Throwable t) {
                        //NOP
                    }
                }
            });
        } catch (Throwable t) {
            Log.debug("CSS error: putObject callback: " + myUserId+":"+myBucketName+":"+myObjectKey+":"+myTempObjectName+" ***" + t.toString());
        }
    }

    public void putObjectStage2(PutObjectType request, SbxPutRequest req) throws EucalyptusCloudException {
        /////////////////////////////////////////////////
        // receive payload
        /////////////////////////////////////////////////
        String userId = request.getUserId();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();

        // writes are unconditional
        WalrusDataQueue<WalrusDataMessage> putQueue = req.messenger.getQueue(req.key, req.randomKey);
        try {
            boolean isTimeout = true;
            boolean bEOF = false;
            while ((isTimeout = true) && (req.dataMessage = putQueue.poll(SbxPutRequest.DATAQUEUE_TIMEOUT, TimeUnit.SECONDS)) != null) {
                if (Contexts.SBXSVR) {
                    try {
                        String myaccount = userId;
                        String myinstid = request.getMetaInstId();
                        WalrusControl.walrusManager.callWalrusHeartBeat(myaccount, myinstid, "PUTOBJECT2");
                    } catch (Throwable t) {
                        Log.debug("CSS callWalrusHeartBeat error (putObject2) " + "***" + t.toString());
                    }
                }
                //reset timeout check
                isTimeout = false;
                //reset timeout count
                req.updatetime();
                // duplicate put object at a same time
                if (putQueue.getInterrupted()) {
                    if (WalrusDataMessage.isEOF(req.dataMessage)) {
                        bEOF = true;
                        putQueue.clear();
                        SbxPayloadLog.log(req, req.type, "Payload", "EOF", "Interrupt");
                        break;
                    } else {
                        SbxPayloadLog.log(req, req.type, "Payload", ".", "Interrupt");
                    }
                    continue;
                }
                if (WalrusDataMessage.isStart(req.dataMessage)) {
                    req.digest = Digest.MD5.get();
                    req.tempObjectName = UUID.randomUUID().toString() + ".tmp";
                    try {
                        req.fileIO = storageManager.prepareForWrite(bucketName, req.tempObjectName);
                    }finally {
                        SbxPayloadLog.log(req, req.type, "Payload", "START", ".");
                        // To garbage collection
                        addPutObjectCallback(req, req.channel, (FileIO)req.fileIO, req.messenger, req.key, req.randomKey, putQueue, userId, bucketName, objectKey, req.tempObjectName);
                    }
                } else if (WalrusDataMessage.isEOF(req.dataMessage)) {
                    bEOF = true;
                    putQueue.clear();
                    SbxPayloadLog.log(req, req.type, "Payload", "EOF", ".");
                    break;
                } else {
                    assert (WalrusDataMessage.isData(req.dataMessage));
                    byte[] data = req.dataMessage.getPayload();
                    // start writing object (but do not commit yet)
                    try {
                        if (req.fileIO != null) {
                            ((FileIO)req.fileIO).write(data);
                        }
                    } catch (IOException ex) {
                        LOG.error(ex);
                    } finally {
                        SbxPayloadLog.log(req, req.type, "Payload", ".", ".");
                    }
                    // calculate md5 on the fly
                    req.size += data.length;
                    if (req.digest != null)
                        req.digest.update(data);
                }
            }// while
            // pubObject is interrupted
            if (putQueue.getInterrupted()) {
                if (bEOF) {
                    if (req.fileIO != null) ((FileIO)req.fileIO).finish();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
                    if (file.exists() == true) {
                        ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                        objectDeleter.start();
                    }
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Duplicate putObject Error (EOF)", true);
                }
                if (req.disconnect) {
                    if (req.fileIO != null) ((FileIO)req.fileIO).finish();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
                    if (file.exists() == true) {
                        ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                        objectDeleter.start();
                    }
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Duplicate putObject Error (DISCONNECT)", true);
                }
            }
            // pubObject is timeout
            if (isTimeout) {
                if (req.disconnect) {
                    if (req.fileIO != null) ((FileIO)req.fileIO).finish();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
                    if (file.exists() == true) {
                        ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                        objectDeleter.start();
                    }
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Read Payload Timeout (DISCONNECT)", true);
                }
                if (req.idletimeout()) {
                    if (req.fileIO != null) ((FileIO)req.fileIO).finish();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
                    if (file.exists() == true) {
                        ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                        objectDeleter.start();
                    }
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Read Payload Timeout (IDLE)", true);
                }
                SbxPutJobQueue.put(req);
                /////////////////////////////////////////////////
                req.reqStage = SbxPutRequest.STAGE.S2;
                /////////////////////////////////////////////////
                return;
            }
        } catch (AccessDeniedException ex) {
            throw ex;
        } catch (Throwable t) {
            if (req.fileIO != null) ((FileIO)req.fileIO).finish();
            req.messenger.removeQueue(req.key, req.randomKey);
            File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
            if (file.exists() == true) {
                ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                objectDeleter.start();
            }
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }

        try {
            if (req.fileIO != null) ((FileIO)req.fileIO).finish();
        } catch (Throwable t) {
            LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + t.toString());
        }
        /////////////////////////////////////////////////
        req.reqStage = SbxPutRequest.STAGE.UPDATE;
        /////////////////////////////////////////////////
    }//putObjectStage2

    public void putObjectStageUpdate(PutObjectType request, SbxPutRequest req) throws EucalyptusCloudException {
        /////////////////////////////////////////////////
        // Insert or Update object
        /////////////////////////////////////////////////
        String userId = request.getUserId();
        String bucketName = request.getBucket();
        String objectKey = request.getKey();

        EntityWrapper<ObjectInfo> dbObject = WalrusControl.getEntityWrapper();
        try {
            if (req.digest != null)
                req.md5 = Hashes.bytesToHex(req.digest.digest());
            req.lastModified = new Date();
            ObjectInfo searchObject = new ObjectInfo(bucketName, objectKey);
            searchObject.setVersionId(req.versionId);
            List<ObjectInfo> objectInfos = dbObject.query(new ObjectInfo(bucketName,
                    objectKey));
            for (ObjectInfo objInfo : objectInfos) {
                objInfo.setLast(false);
            }
            ObjectInfo foundObject = null;
            ObjectInfo objectInfo = null;
            try {
                // bbAddObject should be false
                foundObject = dbObject.getUnique(searchObject);
                if (foundObject.canWriteACP(userId)) {
                    List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
                    foundObject.addGrants(userId, grantInfos, req.accessControlList);
                    foundObject.setGrants(grantInfos);
                }
            } catch (EucalyptusCloudException ex) {
                // bbAddObject should be true
                if (req.bAddObject == false) {
                    // After received payload, the original object is missing (deleted or renamed)
                    dbObject.rollback();
                    ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                    objectDeleter.start();
                    req.messenger.removeQueue(req.key, req.randomKey);
                    throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + "Unable to update object", true);
                }

                objectInfo = new ObjectInfo(bucketName, objectKey);
                objectInfo.setOwnerId(userId);
                List<GrantInfo> grantInfos = new ArrayList<GrantInfo>();
                objectInfo.addGrants(userId, grantInfos, req.accessControlList);
                objectInfo.setGrants(grantInfos);
                objectInfo.setObjectName(req.objectName);
                objectInfo.setSize(0L);
                foundObject = objectInfo;
            }

            int objSeq = -1;
            if (Contexts.SBXSVR) {
                try {
                    if (StringUtils.isNotEmpty(req.objSeq)) {
                        objSeq = Integer.parseInt(req.objSeq);
                    }
                } catch (Throwable t) {
                    throw new AccessDeniedException("GetObjSeq", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
                }
            }
            if (objSeq == -1) {
                objSeq = 0;
            } else {
                objSeq++;
            }

            foundObject.setVersionId(req.versionId);
            foundObject.replaceMetaData(request.getMetaData());
            foundObject.setEtag(req.md5);
            foundObject.setSize(req.size);
            foundObject.setLastModified(req.lastModified);
            foundObject.setStorageClass("STANDARD");
            foundObject.setContentType(request.getContentType());
            foundObject.setContentDisposition(request.getContentDisposition());
            foundObject.setLast(true);
            foundObject.setDeleted(false);
            foundObject.setObjSeq(objSeq);
            req.reply.setSize(req.size);

            String op = null;
            if (req.bAddObject) {
                dbObject.add(foundObject);
                op = "Create";
            } else {
                op = "Create";
                //mac: to change op to modify
                // op = "Modify";
            }

            if (Contexts.SBXSVR) {
                // syncid must LOCK
                if (req.reqId != null) {
                    if (SbxDB.syncidlock(req.syncid) == false) {
                        throw new AccessDeniedException("GetSyncIdError", userId + ":" + bucketName + ":" + objectKey + "*** syncid=" + req.syncid + " is not locked (may be released too early)", true);
                    }
                }

                String reqid = request.getMetaReqId();
                if (reqid == null) reqid = "null";
                //LOG.debug("CSS writeEvent " + op + "): " + userId+":"+bucketName+":"+objectKey);
                this.writeEvent(
                        dbObject.getEntityManager(),
                        request.getMetaClientType(),
                        userId,
                        op,
                        foundObject.getObjectKey(),
                        foundObject.getSize(),
                        strSBXContentTypeFolder.equals(foundObject.getContentType()),
                        req.syncid,
                        req.mtime,
                        reqid, objSeq);
            }

            String[] backup = new String[1];
            try {
                storageManager.renameObject(backup, bucketName, req.tempObjectName, req.objectName);
            } catch (Throwable t) {
                storageManager.renameObjectRollback(backup, bucketName, req.tempObjectName, req.objectName);
                throw t;
            }
            try {
                dbObject.commit();
            } catch (Throwable t) {
                storageManager.renameObjectRollback(backup, bucketName, req.tempObjectName, req.objectName);
                throw t;
            }
            try {
                storageManager.confirm(backup);
            } catch (Throwable t) {
                //NOP
            }

            try {
                if (req.bucket.isVersioningEnabled()) {
                    req.reply.setVersionId(req.versionId);
                }
                if (WalrusProperties.trackUsageStatistics) {
                    walrusStatistics.updateBytesIn(req.size);
                    walrusStatistics.updateSpaceUsed(req.size);
                }
                if (req.logData != null) {
                    req.logData.setObjectSize(req.size);
                    updateLogData(req.bucket, req.logData);
                    req.logData.setTurnAroundTime(Long.parseLong(new String(req.dataMessage.getPayload())));
                }
            } catch (Throwable t) {
                LOG.debug("Error Update Info: " + req.key);
            }

            // restart all interrupted puts
            WalrusMonitor monitor = req.messenger.getMonitor(req.key);
            synchronized (monitor) {
                monitor.setLastModified(req.lastModified);
                monitor.setMd5(req.md5);
                monitor.notifyAll();
            }
            req.messenger.removeMonitor(req.key);
            req.messenger.removeQueue(req.key, req.randomKey);
            LOG.info("Transfer complete: " + req.key);
        } catch (Throwable t) {
            LOG.debug(userId + ":" + bucketName + ":" + objectKey + "***" + t.toString());
            dbObject.rollback();
            req.messenger.removeQueue(req.key, req.randomKey);
            File file = new File(storageManager.getObjectPath(bucketName, req.tempObjectName));
            if (file.exists() == true) {
                ObjectDeleter objectDeleter = new ObjectDeleter(bucketName, req.tempObjectName, -1L);
                objectDeleter.start();
            }
            throw new AccessDeniedException("CssError", userId + ":" + bucketName + ":" + objectKey + "***" + t.toString(), true);
        }

        try {
            req.reply.setEtag(req.md5);
            req.reply.setLastModified(DateUtils.format(req.lastModified.getTime(), DateUtils.ISO8601_DATETIME_PATTERN)
                    + ".000Z");
        } catch (Throwable t) {
            LOG.debug("Error Set Put Reply: " + req.key);
        }

        /////////////////////////////////////////////////
        req.reqStage = SbxPutRequest.STAGE.FINAL;
        /////////////////////////////////////////////////
    }

    public PutObjectResponseType putObject(PutObjectType request, SbxPutRequest r) throws EucalyptusCloudException {
        //do putObject
        if (r.reqStage == SbxPutRequest.STAGE.START) {
            putObjectStage0(request, r);
        }
        if (r.reqStage == SbxPutRequest.STAGE.S0) {
            putObjectStage1(request, r);
        }
        // Timeout Put will be in this stage for a long time
        if (r.reqStage == SbxPutRequest.STAGE.S1 || r.reqStage == SbxPutRequest.STAGE.S2) {
            putObjectStage2(request, r);
        }
        if (r.reqStage == SbxPutRequest.STAGE.UPDATE) {
            putObjectStageUpdate(request, r);
        }
        //return reply
        if (r.reqStage == SbxPutRequest.STAGE.FINAL) {
            return r.reply;
        }
        return null;
    }//staged putObject

    //mac: This method is used for RenameObject
    public PutObjectResponseType putObject(PutObjectType request) throws EucalyptusCloudException {
        //call renameObject
        if(request.getRenameto()!=null){return this.renameObject(request, null);}

        return putObjectRefactor(request);
    }
}
