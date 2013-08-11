package kissmydisc.repricer.command;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.DBAccessor;
import kissmydisc.repricer.dao.DBException;

public class CommandManager extends DBAccessor {

    private int lastProcessed = -1;

    private static final Log log = LogFactory.getLog(CommandManager.class);

    private Command constructCommand(final ResultSet rs) throws Exception {
        Command c = null;
        if (rs.next()) {
            int id = rs.getInt("ID");
            String command = rs.getString("COMMAND");
            String metadata = rs.getString("METADATA");
            Date date = rs.getDate("DATE");
            if (id != -1) {
                lastProcessed = id;
            }
            if ("PROCESS_INVENTORY".equals(command)) {
                c = new ProcessInventoryCommand(id, metadata, date);
            } else if ("START_REPRICER".equals(command)) {
                c = new StartRepricerCommand(id, metadata, date);
            } else if ("STOP_REPRICER".equals(command)) {
                c = new StopRepricerCommand(id, metadata, date);
            } else if ("BLACK_LIST".equals(command)) {
                c = new BlacklistASINCommand(id, metadata, date);
            } else if ("PAUSE_REPRICER".equals(command)) {
                c = new PauseRepricerCommand(id, metadata, date);
            } else if ("CONTINUE_REPRICER".equals(command)) {
                c = new ContinueRepricer(id, metadata, date);
            } else if ("ADD_ASSOCIATION".equals(command)) {
                c = new AddAssociationCommand(id, metadata, date);
            } else if ("CREATE_LISTING".equals(command)) {
                c = new CreateListingsCommand(id, metadata, date);
            } else if ("ADD_ITEMS_TO_INVENTORY".equals(command)) {
                c = new AddItemsToInventoryCommand(id, metadata, date);
            } else {
                log.info("Unknown command [command=" + command + ", id=" + id + "], ignoring!");
            }
        }
        rs.close();
        return c;
    }

    public Command getCommand() throws Exception {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            String query = "select * from (select * from commands where status = 'NEW' and id > ? order by id) TEMP limit 1";
            statement = conn.prepareStatement(query);
            statement.setInt(1, lastProcessed);
            ResultSet result = statement.executeQuery();
            return constructCommand(result);
        } catch (Exception e) {
            throw new DBException("Error while retrieving from DB", e);
        } finally {
            if (statement != null) {
                statement.close();
            }
            releaseConnection();
        }
    }
}
