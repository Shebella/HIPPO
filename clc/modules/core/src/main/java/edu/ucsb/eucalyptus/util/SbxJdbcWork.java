package edu.ucsb.eucalyptus.util;

import org.hibernate.jdbc.Work;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/4
 * Time:  2:44
 * To change this template use File | Settings | File Templates.
 */
public class SbxJdbcWork implements Work {
    private String account;
    private String reqid;
    private String sql;
    public boolean status;

    public SbxJdbcWork(String account, String reqid, String sql) {
        this.account = account;
        this.reqid = reqid;
        this.sql = sql;
    }

    @Override
    public void execute( Connection connection ) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        status = false;
        try {
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, account);
            stmt.setString(2, reqid);
            rs = stmt.executeQuery();
            if (rs != null) {
                if (rs.isBeforeFirst()) status = true;
            }
        } finally {
            try { if (rs != null) rs.close(); } catch (Throwable t) {/*NOP*/}
            try { if (stmt != null) stmt.close(); } catch (Throwable t) {/*NOP*/}
        }
    }
}
