package edu.ucsb.eucalyptus.util;

import org.hibernate.jdbc.Work;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/7/4
 * Time:  2:37
 * To change this template use File | Settings | File Templates.
 */
public class SbxSyncidWork implements Work {
    private long syncid;
    private String sql;
    public boolean status;

    public SbxSyncidWork(long syncid, String sql) {
        this.syncid = syncid;
        this.sql = sql;
    }

    @Override
    public void execute( Connection connection ) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        status = false;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setLong(1, syncid);
            rs = stmt.executeQuery();
            if (rs != null) {
                if (rs.isBeforeFirst()) {
                    rs.next();
                    String str = rs.getString(1);
                    if (str != null && str.trim().equals("LOCK")) status = true;
                }
            }
        } finally {
            try { if (rs != null) rs.close(); } catch (Throwable t) {/*NOP*/}
            try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
        }
    }
}
