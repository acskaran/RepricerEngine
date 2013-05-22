package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.RepriceReport;

public class RepriceReportDAO extends DBAccessor {

    public RepriceReportDAO(final Connection conn) {
        super(conn);
    }

    public RepriceReportDAO() {
        super();
    }

    public void addItems(final List<RepriceReport> items) throws DBException {
    	//ALTER TABLE `repricer_reports`
    	//ADD COLUMN `audit_trail` VARCHAR(300) NOT NULL AFTER `formula_id`;
        String insertStatement = "insert into repricer_reports (PRICE, QUANTITY, FORMULA_ID, INVENTORY_ITEM_ID, REPRICE_ID, AUDIT_TRAIL) values (?, ?, ?, ?, ?, ?)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertStatement);
            for (RepriceReport item : items) {
                st.setFloat(1, item.getPrice());
                st.setInt(2, item.getQuantity());
                st.setInt(3, item.getFormulaId());
                st.setLong(4, item.getInventoryItemId());
                st.setLong(5, item.getRepriceId());
                st.setString(6, item.getAuditTrail());
                st.addBatch();
            }
            st.executeBatch();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

}
