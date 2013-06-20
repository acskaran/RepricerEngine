package kissmydisc.repricer.command;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.engine.RepricerEngine;

public class PauseRepricerCommand extends Command {

    private static final Log log = LogFactory.getLog(PauseRepricerCommand.class);

    private String region;

    public PauseRepricerCommand(final int id, final String message, final Date date) {
        super(id, date);
        String[] mds = message.split("\t");
        for (String md : mds) {
            if (md.startsWith("Region=")) {
                this.region = md.substring("Region=".length());
            }
        }
    }

    @Override
    public void execute() {
        CommandDAO cdao = new CommandDAO();
        try {
            cdao.setStatus(getCommandId(), "STARTED");
            if (region != null) {
                RepricerEngine.getInstance().pauseRepricer(region);
            }
            cdao.setStatus(getCommandId(), "COMPLETED");
        } catch (DBException e) {
            try {
                cdao.setStatus(getCommandId(), "ERROR");
            } catch (DBException e1) {
            }
            log.error("Error Pausing the repricer.", e);
        }
    }

}
