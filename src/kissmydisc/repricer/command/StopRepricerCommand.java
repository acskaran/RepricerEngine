package kissmydisc.repricer.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.engine.RepricerEngine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StopRepricerCommand extends Command {

    private static final Log log = LogFactory.getLog(StartRepricerCommand.class);

    private List<String> regions = new ArrayList<String>();

    private static List<String> ALL_REGIONS = Arrays.asList(new String[] { "JP", "US", "CA", "UK", "DE", "FR" });

    public StopRepricerCommand(int id, String metadata, Date date) {
        super(id, date);
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("Regions=")) {
                String regs = md.substring("Regions=".length());
                if ("ALL".equals(regs)) {
                    regions = ALL_REGIONS;
                } else {
                    String[] rgs = regs.split(",");
                    regions = Arrays.asList(rgs);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "StopRepricerCommand [regions=" + regions + ", commandId=" + getCommandId() + ", Date()=" + getDate()
                + "]";
    }

    @Override
    public void execute() {
        try {
            CommandDAO cdao = new CommandDAO();
            cdao.setStatus(getCommandId(), "STARTED");
            RepricerEngine.getInstance().stopRepricer(regions);
            log.info("Issued stop repricer command.");
            cdao.setStatus(getCommandId(), "COMPLETED");
        } catch (DBException e) {
            log.error("Error stopping the repricer.", e);
        }
    }
}
