package kissmydisc.repricer.dao;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import kissmydisc.repricer.utils.AppConfig;

import org.apache.http.annotation.NotThreadSafe;

import com.mchange.v2.c3p0.ComboPooledDataSource;

@NotThreadSafe
public class DBAccessor {

    private Connection conn = null;

    private boolean myConnection = true;

    public DBAccessor(Connection conn) {
        this.conn = conn;
        this.myConnection = false;
    }

    public DBAccessor() {
    }

    protected void releaseResultSet(ResultSet rs) throws DBException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new DBException(e);
            }
        }
    }

    protected Connection getConnection() throws DBException {
        if (conn == null) {
            conn = DBConnectionFactory.getConnection();
            return conn;
        } else {
            return conn;
        }
    }

    protected void releaseStatement(Statement st) throws DBException {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                throw new DBException(e);
            }
        }
    }

    protected void releaseConnection() throws DBException {
        if (myConnection) {
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (Exception e) {
                    throw new DBException("Error closing the connection", e);
                }
                conn = null;
            }
        }
    }

    static class DBConnectionFactory {
        private static ComboPooledDataSource cpds = null;
        private static boolean initialized = false;

        static {
            if (!initialized) {
                try {
                    cpds = new ComboPooledDataSource();
                    cpds.setDriverClass("com.mysql.jdbc.Driver");
                    String host = AppConfig.getString("DatabaseHost");
                    String database = AppConfig.getString("DatabaseName");
                    int port = AppConfig.getInteger("DatabasePort", 3306);
                    cpds.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database
                            + "?useServerPrepStmts=false&rewriteBatchedStatements=true");
                    cpds.setUser(AppConfig.getString("DatabaseUsername"));
                    cpds.setPassword(AppConfig.getString("DatabasePassword"));
                    cpds.setMaxPoolSize(10);
                    cpds.setMinPoolSize(2);
                    cpds.setNumHelperThreads(10);
                    cpds.setAcquireIncrement(2);
                    cpds.setIdleConnectionTestPeriod(3600);
                    cpds.setMaxConnectionAge(3 * 3600);
                    initialized = true;
                } catch (PropertyVetoException ex) {
                    // handle exception...not important.....
                }
            }
        }

        public synchronized static Connection getConnection() throws DBException {
            if (!initialized) {
                throw new DBException("DBConnectionFactory not initialized!!");
            }
            try {
                return cpds.getConnection();
            } catch (SQLException e) {
                throw new DBException("Error while creating connection", e);
            }
        }
    }

}
