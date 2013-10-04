package edu.ucsb.eucalyptus.util;

import org.hibernate.jdbc.Work;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/9/6
 * Time: 2:31
 * To change this template use File | Settings | File Templates.
 */
public class SbxContainObjectsWork implements Work {
    private String sql;
    private String account;
    private String bucket;
    private String objkey;
    public boolean bExists = false;

    public SbxContainObjectsWork(String account, String bucket, String objkey, String sql) {
        this.sql = sql;
        this.account = account;
        this.bucket = bucket;
        this.objkey = objkey;
    }

    @Override
    public void execute( Connection connection ) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        bExists = false;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, account);
            stmt.setString(2, bucket);
            stmt.setString(3, objkey);
            stmt.setString(4, objkey+"/");
            rs = stmt.executeQuery();
            if (rs != null) {
                if (rs.isBeforeFirst()) {
                    bExists = true;
                }
            }
        } finally {
            try { if (rs != null) rs.close(); } catch (Throwable t) {/*NOP*/}
            try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
        }
    }
}