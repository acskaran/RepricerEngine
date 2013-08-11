package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class CreateListingStatusDAO extends DBAccessor {

    public void updateStatus(final String region, final String status, final String stage) throws Exception {
        String insertQuery = "insert into create_listing_status (REGION, STATUS, STAGE) values (?, ?, ?) ON DUPLICATE KEY UPDATE ";
        insertQuery += "  STATUS = values(STATUS), STAGE = values(STAGE) ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setString(1, region);
            st.setString(2, status);
            st.setString(3, stage);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void updateStatus(final String region, final String status) throws Exception {
        String insertQuery = "insert into create_listing_status (REGION, STATUS) values (?, ?) ON DUPLICATE KEY UPDATE ";
        insertQuery += "  STATUS = values(STATUS) ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setString(1, region);
            st.setString(2, status);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void completeCreateListing(final String region) throws Exception {
        String insertQuery = "insert into create_listing_status (REGION, STATUS, END_TIME) values (?, ?, ?) ON DUPLICATE KEY UPDATE ";
        insertQuery += "  STATUS = values(STATUS)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setString(1, region);
            st.setString(2, "COMPLETED");
            st.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void startCreateListing(final String region) throws Exception {
        String insertQuery = "insert into create_listing_status (REGION, STATUS, STAGE, STARTED_TIME, END_TIME) values (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ";
        insertQuery += "  STATUS = values(STATUS), STAGE = values(STAGE), STARTED_TIME = values(STARTED_TIME), END_TIME = values(END_TIME) ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            int index = 1;
            st.setString(index++, region);
            st.setString(index++, "COMPLETED");
            st.setString(index++, null);
            st.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
            st.setDate(index++, null);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

}
