package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.utils.Pair;

public class InventoryItemDAO extends DBAccessor {
    public InventoryItemDAO(final Connection conn) {
        super(conn);
    }

    public InventoryItemDAO() {
        super();
    }

    public void addItems(final List<InventoryFeedItem> items) throws DBException {
        String insertStatement = "insert into inventory_items (SKU, PRODUCT_ID, INVENTORY_ID, QUANTITY, PRICE, ITEM_CONDITION, INVENTORY_REGION, REGION_PRODUCT, OBI) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertStatement);
            for (InventoryFeedItem item : items) {
                int index = 1;
                st.setString(index++, item.getSku());
                st.setString(index++, item.getProductId());
                st.setLong(index++, item.getInventoryId());
                st.setInt(index++, item.getQuantity());
                st.setFloat(index++, item.getPrice());
                st.setInt(index++, item.getCondition());
                st.setString(index++, item.getRegion());
                st.setString(index++, item.getRegionProductId());
                st.setBoolean(index++, item.getObiItem());
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

    private Map<String, Float> getLowestAmazonPrice(final long inventoryId, List<String> productIds) throws DBException {
        String selectQuery = "select product_id, lowest_amazon_price from inventory_items where inventory_id = ? and inventory_region = ? and product_id in ( ?  ";
        String query = "";
        for (int i = 0; i < productIds.size() - 1; i++) {
            query += ", ? ";
        }
        selectQuery += query + " )";
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            int index = 1;
            st.setLong(index++, inventoryId);
            st.setString(index++, "JP");
            for (String productId : productIds) {
                st.setString(index++, productId);
            }
            rs = st.executeQuery();
            Map<String, Float> map = new HashMap<String, Float>();
            while (rs.next()) {
                String sku = rs.getString("PRODUCT_ID");
                Float lap = rs.getFloat("LOWEST_AMAZON_PRICE");
                map.put(sku, lap);
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

    public void updateLowestAmazonPrice(final Map<Long, Float> skuPriceMap) throws DBException {
        String selectQuery = "update inventory_items set lowest_amazon_price = ? where id = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            for (Map.Entry<Long, Float> e : skuPriceMap.entrySet()) {
                st.setFloat(1, e.getValue());
                st.setLong(2, e.getKey());
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

    public Pair<List<InventoryFeedItem>, String> getMatchingItems(final long inventoryId, final String region,
            String moreToken, final int limit) throws DBException {
        String selectStatement = "select * from inventory_items where inventory_id = ? and region_product like ? ";
        if (moreToken == null) {
            moreToken = region + "_";
        }
        if (moreToken != null) {
            selectStatement += " and region_product >= ? ";
        }
        selectStatement += " limit ?";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectStatement);
            int index = 1;
            st.setLong(index++, inventoryId);
            String regionProduct = region + "_%";
            st.setString(index++, regionProduct);
            if (moreToken != null) {
                st.setString(index++, moreToken);
            }
            st.setInt(index++, limit);
            rs = st.executeQuery();
            int count = 0;
            List<InventoryFeedItem> result = new ArrayList<InventoryFeedItem>();
            String lastProcessed = null;
            while (rs.next()) {
                InventoryFeedItem item = new InventoryFeedItem();
                item.setInventoryItemId(rs.getLong("ID"));
                item.setCondition(rs.getInt("ITEM_CONDITION"));
                item.setInventoryId(rs.getLong("INVENTORY_ID"));
                item.setLowestAmazonPrice(rs.getFloat("LOWEST_AMAZON_PRICE"));
                item.setPrice(rs.getFloat("PRICE"));
                item.setProductId(rs.getString("PRODUCT_ID"));
                item.setQuantity(rs.getInt("QUANTITY"));
                item.setRegion(rs.getString("INVENTORY_REGION"));
                item.setSku(rs.getString("SKU"));
                lastProcessed = rs.getString("REGION_PRODUCT");
                item.setObiItem(rs.getBoolean("OBI"));
                item.setRegionProductId(lastProcessed);
                result.add(item);
                count++;
            }
            if (count < limit) {
                lastProcessed = null;
            }
            return new Pair<List<InventoryFeedItem>, String>(result, lastProcessed);
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public List<InventoryFeedItem> getMatchingItems(final long inventoryId, final String region, final String productId)
            throws DBException {
        String selectStatement = "select * from inventory_items where inventory_id = ? and region_product = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectStatement);
            int index = 1;
            st.setLong(index++, inventoryId);
            String regionProduct = region + "_" + productId;
            st.setString(index++, regionProduct);
            rs = st.executeQuery();
            int count = 0;
            List<InventoryFeedItem> result = new ArrayList<InventoryFeedItem>();
            while (rs.next()) {
                InventoryFeedItem item = new InventoryFeedItem();
                item.setInventoryItemId(rs.getLong("ID"));
                item.setCondition(rs.getInt("ITEM_CONDITION"));
                item.setInventoryId(rs.getLong("INVENTORY_ID"));
                item.setLowestAmazonPrice(rs.getFloat("LOWEST_AMAZON_PRICE"));
                item.setPrice(rs.getFloat("PRICE"));
                item.setProductId(rs.getString("PRODUCT_ID"));
                item.setQuantity(rs.getInt("QUANTITY"));
                item.setRegion(rs.getString("INVENTORY_REGION"));
                item.setObiItem(rs.getBoolean("OBI"));
                item.setSku(rs.getString("SKU"));
                result.add(item);
                count++;
            }
            return result;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void updatePriceAndQuantity(long inventoryItemId, double price, int quantity, double oldPrice,
            int oldQuantity) throws DBException {
        String insertStatement = "update inventory_items set price = ?, quantity = ?, old_price = ?, old_quantity = ? where id = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertStatement);
            st.setFloat(1, (float) price);
            st.setInt(2, quantity);
            st.setFloat(3, (float) oldPrice);
            st.setInt(4, oldQuantity);
            st.setLong(5, inventoryItemId);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }
}
