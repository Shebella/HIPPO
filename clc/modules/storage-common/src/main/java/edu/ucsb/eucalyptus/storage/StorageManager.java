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
/*
 *
 * Author: Sunil Soman sunils@cs.ucsb.edu
 */

package edu.ucsb.eucalyptus.storage;

import java.io.IOException;
import java.util.List;

import com.eucalyptus.context.SbxRequest;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;

import com.eucalyptus.util.EucalyptusCloudException;

import edu.ucsb.eucalyptus.cloud.BucketLogData;
import edu.ucsb.eucalyptus.msgs.WalrusDataGetRequestType;
import edu.ucsb.eucalyptus.storage.fs.FileIO;

public interface StorageManager {

    public void checkPreconditions() throws EucalyptusCloudException;

    public boolean bucketExists(String bucket);

    public boolean objectExists(String bucket, String object);
    
    public void createBucket(String bucket) throws IOException;

    public long getSize(String bucket, String object);

    public void deleteBucket(String bucket) throws IOException;

    public void createObject(String bucket, String object) throws IOException;

    public void putObject(String bucket, String object, byte[] base64Data, boolean append) throws IOException;

    public FileIO prepareForRead(String bucket, String object) throws Exception;

    public FileIO prepareForWrite(String bucket, String object) throws Exception;

    public int readObject(String bucket, String object, byte[] bytes, long offset) throws IOException;

    public int readObject(String objectPath, byte[] bytes, long offset) throws IOException;

    public void deleteObject(String bucket, String object) throws IOException;

    public void deleteAbsoluteObject(String object) throws IOException;

    public void copyObject(String sourceBucket, String sourceObject, String destinationBucket, String destinationObject) throws IOException;

    public void renameObject(String bucket, String oldName, String newName) throws IOException;

    public void renameObject(String[] backup, String bucket, String oldName, String newName) throws IOException;
    public void renameObjectRollback(String[] backup, String bucket, String oldName, String newName) throws IOException;
    public void deleteObject(String[] backup, String bucket, String object) throws IOException;
    public void deleteObjectRollback(String[] backup, String bucket, String object) throws IOException;
    public void confirm(String[] backup) throws IOException;

    public String getObjectPath(String bucket, String object);

    public long getObjectSize(String bucket, String object);

    public void sendObject(WalrusDataGetRequestType request, DefaultHttpResponse httpResponse, String bucketName, String objectName, 
			long size, String etag, String lastModified, String contentType, String contentDisposition, Boolean isCompressed, String versionId, BucketLogData logData);

    public void sendObject(WalrusDataGetRequestType request, DefaultHttpResponse httpResponse, String bucketName, String objectName, 
			long start, long end, long size, String etag, String lastModified, String contentType, String contentDisposition, Boolean isCompressed, String versionId, BucketLogData logData);

    public void sendHeaders(WalrusDataGetRequestType request, DefaultHttpResponse httpResponse, Long size, String etag,
			String lastModified, String contentType, String contentDisposition, String versionId, BucketLogData logData);
	
    public void setRootDirectory(String rootDirectory);
}
