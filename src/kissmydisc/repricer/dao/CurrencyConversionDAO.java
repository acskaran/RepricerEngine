package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CurrencyConversionDAO extends DBAccessor {

    public CurrencyConversionDAO(final Connection conn) {
        super(conn);
    }

    public CurrencyConversionDAO() {
        super();
    }

    public Map<String, Float> getCurrencyConversion() throws DBException {
        String selectQuery = "select * from exchange_rates where to_currency = 'USD'";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            HashMap<String, Float> map = new HashMap<String, Float>();
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            rs = st.executeQuery();
            while (rs.next()) {
                String fromCurrency = rs.getString("from_currency");
                Float factor = rs.getFloat("factor");
                map.put(fromCurrency, factor);
            }
            return map;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }
}
