package kissmydisc.repricer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import kissmydisc.repricer.command.Command;
import kissmydisc.repricer.command.CommandManager;
import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.engine.PeriodicRepriceScheduler;
import kissmydisc.repricer.utils.AppConfig;

public class RepricerMain {

    private static Log log;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("log4j.properties");
        AppConfig.initialize("Repricer.properties");
        AmazonAccessor.initialize();
        log = LogFactory.getLog(RepricerMain.class);
        log.info("Process started.");
        CommandManager commandManager = new CommandManager();
        PeriodicRepriceScheduler scheduler = new PeriodicRepriceScheduler();
        Thread periodicScheduler = new Thread(scheduler);
        periodicScheduler.start();
        while (true) {
            Command command;
            try {
                command = commandManager.getCommand();
                if (command == null) {
                    log.info("No new commands, sleeping for 10 seconds.");
                    Thread.sleep(10000);
                } else {
                    log.info("Processing " + command);
                    command.execute();
                    log.info("Completed " + command);
                }
            } catch (Exception e1) {
                log.error("Error processing the commands", e1);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                } // Sleep and retry
            }
        }
    }
}
