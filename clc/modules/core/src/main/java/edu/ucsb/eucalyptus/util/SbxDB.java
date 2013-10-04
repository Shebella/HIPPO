package edu.ucsb.eucalyptus.util;

import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.util.WalrusProperties;
import edu.ucsb.eucalyptus.cloud.entities.ObjectInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.Session;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/4
 * Time:  4:01
 * To change this template use File | Settings | File Templates.
 */
public class SbxDB {
    private static Logger LOG = Logger.getLogger( SbxDB.class );
    public static boolean lookup_opt_log(final String account, final String reqid) {
        Session sess = null;
        boolean bFound = false;
        try {
            EntityWrapper<ObjectInfo> dbObject = new EntityWrapper<ObjectInfo>( WalrusProperties.DB_NAME );
            EntityManager em = dbObject.getEntityManager();
            try {
                sess = ((Session)em.getDelegate());
            } catch (Throwable t) {
                sess = null;
            }

            final String sql="select op,fpth from opt_log where cssact = ? and reqid = ?";
            if (sess != null) {
                SbxJdbcWork w = new SbxJdbcWork(account, reqid, sql);
                sess.doWork(w);
                bFound = w.status;
            } else {
                Query query = em.createNativeQuery(sql);
                query.setParameter(1, account);
                query.setParameter(2, reqid);
                List rs = query.getResultList();
                if (rs.size() > 0) bFound = true;
            }
            dbObject.commit();

        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxDB.lookup_opt_log error : " + account + ":" + reqid + " : " + t);
        } finally {
            try { if (sess != null) sess.close(); } catch (Throwable t) {/*NOP*/}
        }
        return bFound;
    }

    public static boolean syncidlock(final long syncid) {
        Session sess = null;
        boolean bFound = false;
        try {
            EntityWrapper<ObjectInfo> dbObject = new EntityWrapper<ObjectInfo>( WalrusProperties.DB_NAME );
            EntityManager em = dbObject.getEntityManager();
            try {
                sess = ((Session)em.getDelegate());
            } catch (Throwable t) {
                sess = null;
            }

            final String sql="select status from syncinfo where syncid = ?";
            if (sess != null) {
                SbxSyncidWork w = new SbxSyncidWork(syncid, sql);
                sess.doWork(w);
                bFound = w.status;
            } else {
                Query query = em.createNativeQuery(sql);
                query.setParameter(1, syncid);
                List rs = query.getResultList();
                if (rs.size() > 0) {
                    Object str = rs.get(0);
                    if (str != null && str.toString().trim().contains("LOCK")) {
                        bFound = true;
                    }
                }
            }
            dbObject.commit();

        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxDB.syncidlock error : " + syncid + " : " + t);
        } finally {
            try { if (sess != null) sess.close(); } catch (Throwable t) {/*NOP*/}
        }
        return bFound;
    }

    public static int getObjSeq(final String account, final String bucket, final String objkey) {
        Session sess = null;
        int objseq = -1;
        try {
            EntityWrapper<ObjectInfo> dbObject = new EntityWrapper<ObjectInfo>( WalrusProperties.DB_NAME );
            EntityManager em = dbObject.getEntityManager();
            try {
                sess = ((Session)em.getDelegate());
            } catch (Throwable t) {
                sess = null;
            }

            final String sql="select obj_seq from objects where owner_id = ? and bucket_name = ? and object_key = ?";
            if (sess != null) {
                SbxObjSeqWork w = new SbxObjSeqWork(account, bucket, objkey, sql);
                sess.doWork(w);
                objseq = w.objseq;
            } else {
                Query query = em.createNativeQuery(sql);
                query.setParameter(1, account);
                query.setParameter(2, bucket);
                query.setParameter(3, objkey);
                List rs = query.getResultList();
                if (rs.size() > 0) {
                    Object str = rs.get(0);
                    if (StringUtils.isNotEmpty((String)str)) {
                        objseq = Integer.parseInt((String)str);
                    }
                }
            }
            dbObject.commit();

        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxDB.getObjSeq error : " + objseq + " : " + t);
        } finally {
            try { if (sess != null) sess.close(); } catch (Throwable t) {/*NOP*/}
        }
        return objseq;
    }

    public static boolean containObjects(final String account, final String bucket, final String objkey) {
        Session sess = null;
        boolean bExists = false;
        try {
            EntityWrapper<ObjectInfo> dbObject = new EntityWrapper<ObjectInfo>( WalrusProperties.DB_NAME );
            EntityManager em = dbObject.getEntityManager();
            try {
                sess = ((Session)em.getDelegate());
            } catch (Throwable t) {
                sess = null;
            }

            final String sql="select obj_key from objects where owner_id = ? and bucket_name = ? and object_key != ? and object_key ~ ? limit 1";
            if (sess != null) {
                SbxContainObjectsWork w = new SbxContainObjectsWork(account, bucket, objkey, sql);
                sess.doWork(w);
                bExists = w.bExists;
            } else {
                Query query = em.createNativeQuery(sql);
                query.setParameter(1, account);
                query.setParameter(2, bucket);
                query.setParameter(3, objkey);
                query.setParameter(4, objkey+"/");
                List rs = query.getResultList();
                if (rs.size() > 0) {
                    bExists = true;
                }
            }
            dbObject.commit();

        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxDB.containObjects error : " + bExists + " : " + t);
        } finally {
            try { if (sess != null) sess.close(); } catch (Throwable t) {/*NOP*/}
        }
        return bExists;
    }
}