package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import kissmydisc.repricer.model.Command;

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

    public kissmydisc.repricer.model.Command getCommand(final int id) throws DBException {
        Command command = null;
        String getQuery = "select * from commands where id = ?";
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(getQuery);
            st.setInt(1, id);
            rs = st.executeQuery();
            if (rs.next()) {
                command = new Command();
                command.setCommandId(rs.getInt("ID"));
                command.setCommandName(rs.getString("COMMAND"));
                command.setMetadata(rs.getString("METADATA"));
                command.setDate(rs.getDate("DATE"));
                command.setStatus(rs.getString("STATUS"));
            }
        } catch (SQLException e) {
            throw new DBException("Error updating the status", e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
        return command;
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

    public int addCommand(String command, String metadata, String status) throws DBException {
        String updateQuery = "insert into commands (COMMAND, METADATA, STATUS) values (?, ?, ?)";
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(updateQuery, Statement.RETURN_GENERATED_KEYS);
            st.setString(1, command);
            st.setString(2, metadata);
            st.setString(3, status);
            st.executeUpdate();
            int commandId = -1;
            rs = st.getGeneratedKeys();
            if (rs.next()) {
                commandId = rs.getInt(1);
            }
            log.debug("Inserted command: " + commandId + " command " + " [Command=" + command + ", status=" + status
                    + ", metadata= " + metadata + "]");
            return commandId;
        } catch (SQLException e) {
            throw new DBException("Error updating the status", e);
        } finally {
            releaseResultSet(rs);
            releaseStatement(st);
            releaseConnection();
        }
    }
}
