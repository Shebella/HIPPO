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
 * Author: Neil Soman neil@eucalyptus.com
 */
package com.eucalyptus.auth.login;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.eucalyptus.auth.DatabaseWrappedUser;
import com.eucalyptus.auth.UserEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Logger;
import org.apache.xml.security.utils.Base64;

import com.eucalyptus.auth.Groups;
import com.eucalyptus.auth.Users;
import com.eucalyptus.auth.api.BaseLoginModule;
import com.eucalyptus.auth.crypto.Hmac;
import com.eucalyptus.auth.principal.User;

import java.util.ArrayList;

public class WalrusLoginModule extends BaseLoginModule<WalrusWrappedCredentials> {
	private static Logger LOG = Logger.getLogger( WalrusLoginModule.class );
	public WalrusLoginModule() {}

	@Override
	public boolean accepts( ) {
		return super.getCallbackHandler( ) instanceof WalrusWrappedCredentials;
	}

    static Cache<String, User> cache_user = CacheBuilder.newBuilder().maximumSize(1000).build();
	@Override
	public boolean authenticate( WalrusWrappedCredentials credentials ) throws Exception {
        // mac: fake user
        // fakeauth(credentials);
        // if (true) return true;

        // mac: cache user
        User user = null;
        String key_user = credentials.getQueryId();
        try {
            user = cache_user.getIfPresent(key_user);
        } catch (Throwable t) {
            LOG.error("authenticate error: " + t);
        }
        if (user != null) {
            super.setCredential(key_user);
            super.setPrincipal(user);
            return true;
        }

        String signature = credentials.getSignature().replaceAll("=", "");
		user = Users.lookupQueryId(key_user);
		String queryKey = user.getSecretKey( );
		String authSig = checkSignature( queryKey, credentials.getLoginData() );
		if (authSig.equals(signature)) {
			super.setCredential(key_user);
			super.setPrincipal(user);
            //mac : skip lookup groups to improve performance
			//super.getGroups().addAll(Groups.lookupUserGroups( super.getPrincipal()));

            // mac: cache user
            try {
                cache_user.put(key_user, user);
            } catch (Throwable t) {
                LOG.error("authenticate error: " + t);
            }
            return true;
		}
		return false;
	}

    public boolean fakeauth (WalrusWrappedCredentials credentials) throws Exception {
        UserEntity fakeUserEntity = new UserEntity();
        fakeUserEntity.setId("user5");
        fakeUserEntity.setSecretKey("M4HV2A35vcoHW50zUkfRBEqwXRzIOo6fOy1pQ");
        fakeUserEntity.setName("user5");
        fakeUserEntity.setAdministrator(false);
        fakeUserEntity.setEnabled(true);
        fakeUserEntity.setToken("rItFVyJKmINm9O4LfLxuUwLKRgjlITsE5fYfZQcab9d8148045fe2c39945dc661ca36c6fab9a65f");
        User user = new DatabaseWrappedUser(fakeUserEntity);
        super.setCredential(credentials.getQueryId());
        super.setPrincipal(user);
        return true;
    }

	@Override
	public void reset( ) {}

	protected String checkSignature( final String queryKey, final String subject ) throws AuthenticationException
	{
		SecretKeySpec signingKey = new SecretKeySpec( queryKey.getBytes(), Hmac.HmacSHA1.toString() );
		try
		{
			Mac mac = Mac.getInstance( Hmac.HmacSHA1.toString() );
			mac.init( signingKey );
			byte[] rawHmac = mac.doFinal( subject.getBytes() );
			return new String(Base64.encode( rawHmac )).replaceAll( "=", "" );
		}
		catch ( Exception e )
		{
			LOG.error( e, e );
			throw new AuthenticationException( "Failed to compute signature" );
		}
	}
}
