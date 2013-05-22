package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CommandDAO extends DBAccessor {

    private Connection conn;

    private static final Log log = LogFactory.getLog(CommandDAO.class);

    public CommandDAO(final Connection conn) {
        super(conn);
    }

    public CommandDAO() {
        super();
    }

    public void setStatus(final int id, final String status) throws DBException {
        String updateQuery = "update commands set status = ? where id = ?";
        PreparedStatement st = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(1, status);
            st.setInt(2, id);
            int rows = st.executeUpdate();
            log.debug("Updated " + rows + " rows for update " + " [status=" + status + ", id=" + id + "]");
        } catch (SQLException e) {
            throw new DBException("Error updating the status", e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }

    public void addCommand(String command, String metadata, String status) throws DBException {
        String updateQuery = "insert into commands (COMMAND, METADATA, STATUS) values (?, ?, ?)";
        PreparedStatement st = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery);
            st.setString(1, command);
            st.setString(2, metadata);
            st.setString(3, status);
            int rows = st.executeUpdate();
            log.debug("Inserted " + rows + " command " + " [Command=" + command + ", status=" + status + ", metadata= "
                    + metadata + "]");
        } catch (SQLException e) {
            throw new DBException("Error updating the status", e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }
}
