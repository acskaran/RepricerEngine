package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import kissmydisc.repricer.utils.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LatestInventoryDAO extends DBAccessor {

    private static final Log log = LogFactory.getLog(LatestInventoryDAO.class);

    public LatestInventoryDAO(final Connection conn) {
        super(conn);
    }

    public LatestInventoryDAO() {
        super();
    }

    public long getLatestInventory(final String region) throws DBException {
        String selectQuery = "select * from latest_inventory where region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            st.setString(1, region);
            rs = st.executeQuery();
            if (rs.next()) {
                long inventoryId = rs.getLong("INVENTORY_ID");
                return inventoryId;
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public Pair<Long, Long> getLatestInventoryWithCount(final String region) throws DBException {
        String selectQuery = "select * from latest_inventory where region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            st.setString(1, region);
            rs = st.executeQuery();
            if (rs.next()) {
                long inventoryId = rs.getLong("INVENTORY_ID");
                long count = rs.getLong("total_items");
                return new Pair<Long, Long>(inventoryId, count);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void insertLatestInventory(final String region, final Long inventoryId, final long count) throws DBException {
        String insertQuery = "insert into latest_inventory (region, inventory_id, total_items) values (?, ?, ?)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setString(1, region);
            st.setLong(2, inventoryId);
            st.setLong(3, count);
            st.executeUpdate();
            log.debug("Inserted a new row " + region + "," + inventoryId + " into latest_inventory");
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setLatestInventory(String region, long inventoryId, long count) throws Exception {
        String updateQuery = "update latest_inventory set INVENTORY_ID = ?, TOTAL_ITEMS = ? where region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setLong(1, inventoryId);
            st.setLong(2, count);
            st.setString(3, region);
            int rows = st.executeUpdate();
            if (rows == 0) {
                insertLatestInventory(region, inventoryId, count);
            } else {
                log.debug(rows + " rows updated with inventoryId = " + inventoryId);
            }

        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

}
