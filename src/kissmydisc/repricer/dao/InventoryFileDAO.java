package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import kissmydisc.repricer.model.InventoryFeed;

public class InventoryFileDAO extends DBAccessor {

    public InventoryFileDAO(final Connection conn) {
        super(conn);
    }

    public InventoryFileDAO() {
        super();
    }

    public InventoryFeed getInventoryFeed(final long id) throws DBException {
        String selectQuery = "select * from inventory_feeds where id = ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            st.setLong(1, id);
            rs = st.executeQuery();
            if (rs.next()) {
                long id_long = rs.getLong("ID");
                String url = rs.getString("URL");
                InventoryFeed feed = new InventoryFeed();
                feed.setId(id_long);
                feed.setUrl(url);
                return feed;
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
}
