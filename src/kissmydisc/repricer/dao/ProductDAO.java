package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kissmydisc.repricer.model.ProductDetails;

public class ProductDAO extends DBAccessor {

    public ProductDAO(final Connection conn) {
        super(conn);
    }

    public ProductDAO() {
        super();
    }

    /*
     * CREATE TABLE `asin_associations` ( `region` VARCHAR(50) NOT NULL,
     * `product_id` VARCHAR(50) NOT NULL, `jp_product_id` VARCHAR(50) NOT NULL,
     * PRIMARY KEY (`region`, `product_id`) ) COLLATE='latin1_swedish_ci'
     * ENGINE=InnoDB;
     */
    public void addAssociation(final String region, final String asinForRegion, final String targetJPAsin)
            throws Exception {
        String insertQuery = "insert into asin_associations (REGION, PRODUCT_ID, JP_PRODUCT_ID) values (?, ?, ?) on DUPLICATE KEY UPDATE JP_PRODUCT_ID = values(JP_PRODUCT_ID)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setString(1, region);
            st.setString(2, asinForRegion);
            st.setString(3, targetJPAsin);
            st.execute();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public Map<String, String> getAssociation(final String region, List<String> productIds) throws DBException {
        String selectQuery = "select * from asin_associations where REGION = ? and PRODUCT_ID in ( ? ";
        if (productIds.size() > 1) {
            for (int i = 0; i < productIds.size() - 1; i++) {
                selectQuery += ", ? ";
            }
        }
        selectQuery += " ) ";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            int index = 1;
            st.setString(index++, region);
            for (String productId : productIds) {
                st.setString(index++, productId);
            }
            rs = st.executeQuery();
            Map<String, String> associations = new HashMap<String, String>();
            while (rs.next()) {
                String productId = rs.getString("PRODUCT_ID");
                String jpProductId = rs.getString("JP_PRODUCT_ID");
                associations.put(productId, jpProductId);
            }
            return associations;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setProductWeight(Map<String, Float> map) throws DBException {
        String insertQuery = "insert into product_details (PRODUCT_ID, WEIGHT) values (?, ?) ON DUPLICATE KEY UPDATE WEIGHT = values(WEIGHT)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            for (Map.Entry<String, Float> entry : map.entrySet()) {
                st.setString(1, entry.getKey());
                st.setFloat(2, entry.getValue());
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

    public Float getProductWeight(final String sku) throws DBException {
        Map<String, Float> productWeight = getProductWeight(Arrays.asList(sku));
        if (productWeight.size() > 0) {
            return productWeight.get(sku);
        }
        return -1.0F;
    }

    public Map<String, Float> getProductWeight(final List<String> skuList) throws DBException {
        String selectQuery = "select * from product_details where PRODUCT_ID in ( ? ";
        if (skuList.size() > 1) {
            for (int i = 0; i < skuList.size() - 1; i++) {
                selectQuery += ", ? ";
            }
        }
        selectQuery += " ) ";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            int index = 1;
            for (String sku : skuList) {
                st.setString(index++, sku);
            }
            rs = st.executeQuery();
            Map<String, Float> map = new HashMap<String, Float>();
            while (rs.next()) {
                Float weight = rs.getFloat("WEIGHT");
                String sku = rs.getString("PRODUCT_ID");
                map.put(sku, weight);
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

    public Map<String, Boolean> isBlackList(String region, List<String> items) throws DBException {
        if (items.size() == 0) {
            return Collections.emptyMap();
        }
        String selectQuery = "select * from product_blacklist where region = ? and blacklist = 1 and product_id in ( ? ";
        String query = "";
        for (int i = 0; i < items.size() - 1; i++) {
            query += ", ? ";
        }
        selectQuery += query + " )";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            int index = 1;
            st.setString(index++, region);
            for (String item : items) {
                st.setString(index++, item);
            }
            rs = st.executeQuery();
            Map<String, Boolean> map = new HashMap<String, Boolean>();
            while (rs.next()) {
                String productId = rs.getString("PRODUCT_ID");
                map.put(productId, true);
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

    public void whitelist(String region) throws DBException {
        String selectQuery = "update product_blacklist set blacklist = 0 where region = ?";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            int index = 1;
            st.setString(index++, region);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void addToBlacklist(String region, String productId) throws Exception {
        String updateQuery = "update product_blacklist set blacklist = 1 where region = ? and product_id = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            int index = 1;
            st.setString(index++, region);
            st.setString(index++, productId);
            int rows = st.executeUpdate();
            if (rows == 0) {
                releaseStatement(st);
                releaseConnection();
                conn = getConnection();
                st = conn
                        .prepareStatement("insert into product_blacklist (REGION, PRODUCT_ID, BLACKLIST) values (?, ?, ?)");
                st.setString(1, region);
                st.setString(2, productId);
                st.setInt(3, 1);
                int r = st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public Map<String, ProductDetails> getProductDetails(List<String> productIds) throws DBException {
        String query = "select * from product_details where product_id in ( ? ";
        String additional = "";
        if (productIds.size() > 0) {
            for (int i = 1; i < productIds.size(); i++) {
                additional += " , ? ";
            }
        }
        query += additional + " ) ";
        Map<String, ProductDetails> map = new HashMap<String, ProductDetails>();
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(query);
            int index = 1;
            for (String productId : productIds) {
                st.setString(index++, productId);
            }
            rs = st.executeQuery();
            while (rs.next()) {
                String productId = rs.getString("PRODUCT_ID");
                ProductDetails details = new ProductDetails();
                details.setWeight(rs.getFloat("WEIGHT"));
                details.setNewPrice(rs.getFloat("NEW_LOWEST_PRICE"));
                details.setNewQuantity(rs.getInt("NEW_QUANTITY"));
                details.setUsedPrice(rs.getFloat("USED_LOWEST_PRICE"));
                details.setUsedQuantity(rs.getInt("USED_QUANTITY"));
                details.setLastUpdated(rs.getTimestamp("LAST_REFRESHED"));
                map.put(productId, details);
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

    public void updateProductPriceQuantityData(List<ProductDetails> productDetails) throws DBException {
        String query = "insert into product_details (PRODUCT_ID, NEW_LOWEST_PRICE, NEW_QUANTITY, USED_LOWEST_PRICE, USED_QUANTITY, LAST_REFRESHED) values (?, ?, ?, ?, ?, ? ) ON DUPLICATE KEY UPDATE ";
        query += " NEW_LOWEST_PRICE = values(NEW_LOWEST_PRICE), NEW_QUANTITY = values(NEW_QUANTITY), USED_QUANTITY = values(USED_QUANTITY), USED_LOWEST_PRICE = values(USED_LOWEST_PRICE), LAST_REFRESHED = values(LAST_REFRESHED) ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            st = conn.prepareStatement(query);
            for (ProductDetails product : productDetails) {
                int index = 1;
                st.setString(index++, product.getProductId());
                st.setFloat(index++, product.getNewPrice());
                st.setInt(index++, product.getNewQuantity());
                st.setFloat(index++, product.getUsedPrice());
                st.setInt(index++, product.getUsedQuantity());
                st.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
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
