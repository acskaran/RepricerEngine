package kissmydisc.repricer.engine;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.model.RepricerConfiguration;

public class PeriodicRepriceScheduler implements Runnable {

    private static final Log log = LogFactory.getLog(PeriodicRepriceScheduler.class);

    @Override
    public void run() {
        RepricerConfigurationDAO configDAO = new RepricerConfigurationDAO();
        while (true) {
            try {
                List<RepricerConfiguration> repricers = configDAO.getRepricers();
                List<String> toSchedule = new ArrayList<String>();
                for (RepricerConfiguration repricer : repricers) {
                    // Repricer is configured to run in periodic intervals.
                    Date now = new Date(System.currentTimeMillis());
                    // If interval is past, and the status of the repricer
                    // is scheduled.
                    if (repricer.getInterval() != -1 && "SCHEDULED".equals(repricer.getStatus())
                            && repricer.getNextRun() != null && repricer.getNextRun().before(now)) {
                        log.info(repricer.getNextRun() + " now " + now);
                        toSchedule.add(repricer.getRegion());
                        configDAO.setStatus(repricer.getRegion(), "WAITING", "SCHEDULED");
                    }
                }
                if (toSchedule.size() > 0) {
                    log.info("Scheduling " + toSchedule);
                    RepricerEngine.getInstance().scheduleRepricer(toSchedule, "PERIODIC");
                }
            } catch (DBException e) {
                // Nothing to do.
                log.warn("Error in RepriceManager", e);
            }
            try {
                Thread.sleep(60000); // Wait for 1 minute before polling DB.
            } catch (InterruptedException e) {
                // Nothing to do.
            }

        }
    }
}
