package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.Log;

import kissmydisc.repricer.RepricerMain;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.model.RepricerFormula;
import kissmydisc.repricer.utils.Pair;

public class RepricerConfigurationDAO extends DBAccessor {

    /*
     * ALTER TABLE `repricer_formula` ADD COLUMN `obi_quantity_limit` INT(10)
     NULL AFTER `quantity_limit`, ADD COLUMN `obi_formula` VARCHAR(100) NULL
     AFTER `formula`;
     * 
     * 
     * 
     * ALTER TABLE `repricer_formula` ADD COLUMN `default_weight` DOUBLE NOT
     NULL AFTER `obi_formula`, ADD COLUMN `obi_default_weight` DOUBLE NOT NULL
     AFTER `default_weight`;
     * 
     * 
     * 
     * ALTER TABLE `repricer_formula` CHANGE COLUMN `default_weight`
      `default_weight` DOUBLE NOT NULL DEFAULT '-1.0' AFTER `obi_formula`,
      CHANGE COLUMN `obi_default_weight` `obi_default_weight` DOUBLE NOT NULL
      DEFAULT '-1.0' AFTER `default_weight`;
     */

    private static final Log log = LogFactory.getLog(RepricerConfiguration.class);

    public RepricerConfigurationDAO(final Connection conn) {
        super(conn);
    }

    public RepricerConfigurationDAO() {
        super();
    }

    public Map<String, Pair<String, String>> getRepricerMarketplaceAndSeller() throws DBException {
        String selectQuery = "select region, marketplace_id, seller_id from repricer_configuration RC";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        Map<String, Pair<String, String>> repricerConfig = new HashMap<String, Pair<String, String>>();
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            rs = st.executeQuery();
            while (rs.next()) {
                String region = rs.getString("REGION");
                String marketplace = rs.getString("MARKETPLACE_ID");
                String sellerId = rs.getString("SELLER_ID");
                repricerConfig.put(region, new Pair<String, String>(marketplace, sellerId));
            }
            return repricerConfig;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public List<RepricerConfiguration> getRepricers() throws DBException {
        String selectQuery = "select * from repricer_configuration RC, repricer_formula RF where RC.formula_id = RF.formula_id";
        PreparedStatement st = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(selectQuery);
            rs = st.executeQuery();
            List<RepricerConfiguration> repricerConfiguration = new ArrayList<RepricerConfiguration>();
            while (rs.next()) {
                String region = rs.getString("RC.REGION");
                String status = rs.getString("RC.REPRICER_STATUS");
                Timestamp nextRun = rs.getTimestamp("RC.NEXT_RUN");
                int interval = rs.getInt("RC.REPRICER_INTERVAL");
                RepricerConfiguration config = new RepricerConfiguration();
                RepricerFormula formula = new RepricerFormula();
                formula.setFormulaId(rs.getInt("RF.FORMULA_ID"));
                formula.setInitialFormula(rs.getString("RF.FORMULA"));
                formula.setQuantityLimit(rs.getInt("RF.QUANTITY_LIMIT"));
                if (rs.getBoolean("RF.SECOND_LEVEL_REPRICING")) {
                    formula.setSecondLevelRepricing(true);
                    formula.setLowerPriceMarigin(rs.getDouble("RF.LOWER_PRICE_MARIGIN"));
                    formula.setSecondLevelRepricingLowerLimit(rs.getDouble("RF.LOWER_LIMIT"));
                    formula.setSecondLevelRepricingLowerLimitPercent(rs.getDouble("RF.LOWER_LIMIT_PERCENT"));
                    formula.setSecondLevelRepricingUpperLimit(rs.getDouble("RF.UPPER_LIMIT"));
                    formula.setSecondLevelRepricingUpperLimitPercent(rs.getDouble("RF.UPPER_LIMIT_PERCENT"));
                }
                formula.setDefaultObiWeight(rs.getDouble("RF.OBI_DEFAULT_WEIGHT"));
                formula.setDefaultWeight(rs.getDouble("RF.DEFAULT_WEIGHT"));
                formula.setObiFormula(rs.getString("RF.OBI_FORMULA"));
                formula.setObiQuantityLimit(rs.getInt("RF.OBI_QUANTITY_LIMIT"));
                config.setFormula(formula);
                config.setRegion(region);
                config.setStatus(status);
                if (nextRun != null) {
                    config.setNextRun(new Date(nextRun.getTime()));
                }
                config.setMarketplaceId(rs.getString("MARKETPLACE_ID"));
                config.setSellerId(rs.getString("SELLER_ID"));
                config.setInterval(interval);
                config.setCacheRefreshTime(rs.getInt("CACHE_REFRESH_INTERVAL"));
                repricerConfiguration.add(config);
            }
            return repricerConfiguration;
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setStatus(String region, String status, String oldStatus) throws DBException {
        String updateQuery = "update repricer_configuration set repricer_status = ? where region = ? ";
        if (oldStatus != null) {
            updateQuery += " and repricer_status = ?";
        }
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(2, region);
            st.setString(1, status);
            if (oldStatus != null) {
                st.setString(3, oldStatus);
            }
            int rows = st.executeUpdate();
            log.debug(rows + " rows updated for status update query[region=" + region + ", status=" + status + "]");
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setStatus(String region, String status, long repriceId) throws DBException {
        String updateQuery = "update repricer_configuration set repricer_status = ?, latest_reprice_id = ? where region = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(1, status);
            st.setLong(2, repriceId);
            st.setString(3, region);
            int rows = st.executeUpdate();
            log.debug(rows + " rows updated for status update query[region=" + region + ", status=" + status + "]");
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setCompletedOrTerminated(String region, String status) throws DBException {
        String updateQuery = "update repricer_configuration set repricer_status = ?  where region = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(1, status);
            st.setString(2, region);
            int rows = st.executeUpdate();
            log.debug(rows + " rows updated for status update query[region=" + region + ", status=" + status + "]");
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void setNextRun(String region, String status, long date) throws DBException {
        String updateQuery = "update repricer_configuration set repricer_status = ? , next_run = ?  where region = ? ";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(1, status);
            st.setTimestamp(2, new Timestamp(date));
            st.setString(3, region);
            int rows = st.executeUpdate();
            log.debug(rows + " rows updated for status update query[region=" + region + ", status=" + status + "]");
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public RepricerConfiguration getRepricer(String region) throws DBException {
        List<RepricerConfiguration> configs = getRepricers();
        for (RepricerConfiguration config : configs) {
            if (config.getRegion().equals(region)) {
                return config;
            }
        }
        return null;
    }

}
