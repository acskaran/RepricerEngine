package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import kissmydisc.repricer.model.ListingConfiguration;

public class ListingConfigurationDAO extends DBAccessor {
    public ListingConfigurationDAO() {
        super();
    }

    public ListingConfiguration getListingConfiguration(String region) throws DBException {
        ListingConfiguration config = null;
        String query = "select * from listing_configuration where region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(query);
            st.setString(1, region);
            rs = st.executeQuery();
            if (rs.next()) {
                config = new ListingConfiguration();
                config.setExpeditedShipping(rs.getString("EXPEDITED_SHIPPING"));
                config.setItemIsMarketplace(rs.getString("ITEM_IS_MARKETPLACE"));
                config.setItemNoteNew(rs.getString("ITEM_NOTE_NEW"));
                config.setItemNoteObi(rs.getString("ITEM_NOTE_OBI"));
                config.setItemNoteUsed(rs.getString("ITEM_NOTE_USED"));
                config.setWillShipInternationally(rs.getString("WILL_SHIP_INTERNATIONALLY"));
            }
            return config;
        } catch (SQLException e) {
            throw new DBException("Error in GetListingConfiguration", e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }
}
