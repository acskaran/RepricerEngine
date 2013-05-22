package kissmydisc.repricer.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.engine.RepricerEngine;

public class StartRepricerCommand extends Command {

    private static final Log log = LogFactory.getLog(StartRepricerCommand.class);

    private List<String> regions = new ArrayList<String>();

    private String priority = "PERIODIC";

    private static List<String> ALL_REGIONS = Arrays.asList(new String[] { "JP", "US", "CA", "UK", "DE", "FR" });

    public StartRepricerCommand(int id, String metadata, Date date) {
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
            if (md.startsWith("Priority=")) {
                priority = md.substring("Priority=".length());
            }
        }
    }

    @Override
    public void execute() {
        try {
            CommandDAO cdao = new CommandDAO();
            cdao.setStatus(getCommandId(), "STARTED");
            if (!regions.isEmpty()) {
                RepricerEngine.getInstance().scheduleRepricer(regions, priority);
            }
            cdao.setStatus(getCommandId(), "COMPLETED");
        } catch (DBException e) {
            log.error("Error starting the repricer.", e);
        }
    }

    @Override
    public String toString() {
        return "StartRepricerCommand [regions=" + regions + ", priority=" + priority + ", id= " + getCommandId() + "]";
    }
}
