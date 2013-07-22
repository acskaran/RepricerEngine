package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import kissmydisc.repricer.model.PriceQuantityFeed;
import kissmydisc.repricer.model.ProductDetails;

public class KMDInventoryDAO extends KMDDBAccessor {

    public KMDInventoryDAO(final Connection conn) {
        super(conn);
    }

    public KMDInventoryDAO() {
        super();
    }

    public void updateKMDPrice(final List<PriceQuantityFeed> feedList) throws DBException {
        String query = "update catalog set price = ?, quantity = ? where sku = ?";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            st = conn.prepareStatement(query);
            for (PriceQuantityFeed feed : feedList) {
                int index = 1;
                st.setFloat(index++, feed.getPrice());
                st.setInt(index++, feed.getQuantity());
                st.setString(index++, feed.getSku());
                st.addBatch();
            }
            st.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            st.clearBatch();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }
}
