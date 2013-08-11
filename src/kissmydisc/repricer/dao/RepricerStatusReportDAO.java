package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import kissmydisc.repricer.model.RepricerStatus;

public class RepricerStatusReportDAO extends DBAccessor {
    public RepricerStatusReportDAO(Connection conn) {
        super(conn);
    }

    public RepricerStatusReportDAO() {
        super();
    }

    public RepricerStatus addNewRepricer(String region) throws DBException {
        String insertStatement = "insert into repricer_status (REGION, R_STATUS) values (?, ?)";
        PreparedStatement st = null;
        Connection conn;
        conn = getConnection();
        ResultSet rs = null;
        try {
            st = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);
            st.setString(1, region);
            st.setString(2, "RUNNING");
            st.executeUpdate();
            rs = st.getGeneratedKeys();
            if (rs.next()) {
                int id = rs.getInt(1);
                RepricerStatus status = new RepricerStatus();
                status.setRepriceId(id);
                status.setStatus("RUNNING");
                status.setRegion(region);
                status.setTotalCompleted(0);
                status.setTotalRepriced(0);
                status.setTotalScheduled(0);
                status.setRepriceRate(0.0F);
                return status;
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

    public void updateStatus(RepricerStatus status) throws DBException {
        String updateStatement = "update repricer_status set R_STATUS = ?, TOTAL_SCHEDULED = ?, TOTAL_COMPLETED = ?, REPRICE_RATE = ?, PRICE_DOWN = ? , PRICE_UP = ?, QUANTITY_RESET_TO_ZERO = ?, NO_PRICE_CHANGE = ?, LOWEST_PRICE = ? , ELAPSED = ?, LAST_REPRICED_ID = ?, LAST_REPRICED = ?, OBI_QUANTITY_RESET_TO_ZERO = ?, OBI_NO_PRICE_CHANGE = ?, OBI_PRICE_UP = ?, OBI_PRICE_DOWN = ?";
        Timestamp endTime = null;
        if ("COMPLETED".equals(status.getStatus()) || "ERROR".equals(status.getStatus())
                || "TERMINATED".equals(status.getStatus())) {
            updateStatement += ", END_TIME = ? ";
            endTime = new Timestamp(System.currentTimeMillis());
        }
        updateStatement += " where REPRICE_ID = ? ";
        PreparedStatement st = null;
        Connection conn;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateStatement);
            int index = 1;
            st.setString(index++, status.getStatus());
            st.setInt(index++, status.getTotalScheduled());
            st.setInt(index++, status.getTotalCompleted());
            st.setFloat(index++, status.getRepriceRate());
            st.setInt(index++, status.getPriceDown());
            st.setInt(index++, status.getPriceUp());
            st.setInt(index++, status.getQuantityResetToZero());
            st.setInt(index++, status.getNoPriceChange());
            st.setInt(index++, status.getLowestPrice());
            st.setLong(index++, status.getElapsed());
            st.setLong(index++, status.getLastRepricedId());
            st.setString(index++, status.getLastRepriced());
            st.setInt(index++, status.getObiQuantityResetToZero());
            st.setInt(index++, status.getObiNoPriceChange());
            st.setInt(index++, status.getObiPriceUp());
            st.setInt(index++, status.getObiPriceDown());
            // String updateStatement =
            // "update repricer_status set R_STATUS = ?, TOTAL_SCHEDULED = ?, TOTAL_COMPLETED = ?, TOTAL_REPRICED = ?, REPRICE_RATE = ?, PRICE_DOWN = ? , PRICE_UP = ?, QUANTITY_RESET_TO_ZERO = ?, NO_PRICE_CHANGE = ?, LOWEST_PRICE = ? , ELAPSED = ?, LAST_REPRICED_ID = ?, LAST_REPRICED = ?";
            if (endTime != null) {
                st.setTimestamp(index++, endTime);
            }
            st.setLong(index++, status.getRepriceId());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public RepricerStatus getRepricerStatus(String region) throws DBException {
        String selectQuery = "select * from repricer_status RS, repricer_configuration RC where RC.latest_reprice_id = RS.reprice_id and RC.region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            st.setString(1, region);
            rs = st.executeQuery();
            RepricerStatus status = null;
            while (rs.next()) {
                status = new RepricerStatus();
                status.setElapsed(rs.getLong("ELAPSED"));
                status.setLastRepriced(rs.getString("LAST_REPRICED"));
                status.setLastRepricedId(rs.getInt("LAST_REPRICED_ID"));
                status.setLowestPrice(rs.getInt("LOWEST_PRICE"));
                status.setNoPriceChange(rs.getInt("NO_PRICE_CHANGE"));
                status.setPriceDown(rs.getInt("PRICE_DOWN"));
                status.setPriceUp(rs.getInt("PRICE_UP"));
                status.setQuantityResetToZero(rs.getInt("QUANTITY_RESET_TO_ZERO"));
                status.setObiQuantityResetToZero(rs.getInt("OBI_QUANTITY_RESET_TO_ZERO"));
                status.setObiPriceUp(rs.getInt("OBI_PRICE_UP"));
                status.setObiPriceDown(rs.getInt("OBI_PRICE_DOWN"));
                status.setObiNoPriceChange(rs.getInt("OBI_NO_PRICE_CHANGE"));
                status.setRegion(rs.getString("RS.REGION"));
                status.setRepriceId(rs.getInt("RS.REPRICE_ID"));
                status.setTotalCompleted(rs.getInt("RS.TOTAL_COMPLETED"));
                status.setTotalScheduled(rs.getInt("RS.TOTAL_SCHEDULED"));
            }
            return status;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }
}
