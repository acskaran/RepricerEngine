package kissmydisc.repricer.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.utils.Pair;

public class RepricerEngine {

    private static final Log log = LogFactory.getLog(RepricerEngine.class);

    private RepricerScheduler scheduler;

    private Thread schedulerThread;

    private RepricerEngine() {
        scheduler = new RepricerScheduler();
        schedulerThread = new Thread(scheduler);
        schedulerThread.start();
    }

    private static RepricerEngine REPRICER_ENGINE = new RepricerEngine();

    private Map<String, Pair<RepriceWorker, Thread>> MAP_REPRICE_WORKER = new Hashtable<String, Pair<RepriceWorker, Thread>>();

    private Map<Long, List<String>> REPRICER_SCHEDULE = new Hashtable<Long, List<String>>();

    private Map<Long, String> SCHEDULE_METADATA = new Hashtable<Long, String>();

    public static RepricerEngine getInstance() {
        return REPRICER_ENGINE;
    }

    private class RepricerScheduler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // Check if we have any new reprice schedule and start the
                    // work.
                    synchronized (REPRICER_SCHEDULE) {
                        if (REPRICER_SCHEDULE.size() > 0) {
                            if (log.isDebugEnabled()) {
                                log.debug("Repricer Schedule: " + REPRICER_SCHEDULE);
                            }
                            Set<Long> timeList = REPRICER_SCHEDULE.keySet();
                            Long[] scheduleTime = new Long[timeList.size()];
                            int i = 0;
                            for (Long time : timeList) {
                                scheduleTime[i++] = time;
                            }
                            Arrays.sort(scheduleTime);
                            for (Long time : scheduleTime) {
                                Long toSchedule = time;
                                List<String> regionsToSchedule = REPRICER_SCHEDULE.get(toSchedule);
                                if (log.isDebugEnabled()) {
                                    log.debug("Trying to start repricer for " + regionsToSchedule);
                                }
                                String metadata = null;
                                if (SCHEDULE_METADATA.containsKey(toSchedule)) {
                                    metadata = SCHEDULE_METADATA.get(toSchedule);
                                }
                                List<String> regionsToAdd = new ArrayList<String>();
                                synchronized (MAP_REPRICE_WORKER) {
                                    for (String region : regionsToSchedule) {
                                        if ((MAP_REPRICE_WORKER.containsKey(region) && (MAP_REPRICE_WORKER.get(region)
                                                .getSecond() == null || !MAP_REPRICE_WORKER.get(region).getSecond()
                                                .isAlive()))
                                                || !MAP_REPRICE_WORKER.containsKey(region)) {
                                            RepriceWorker repriceWorker = null;
                                            repriceWorker = new RepriceWorker(region, metadata);
                                            Thread thread = new Thread(repriceWorker);
                                            thread.start();
                                            MAP_REPRICE_WORKER.put(region, new Pair<RepriceWorker, Thread>(
                                                    repriceWorker, thread));
                                            log.info("Started repricer for " + region);
                                        } else {
                                            regionsToAdd.add(region);
                                            log.info("Unable to schedule " + region
                                                    + " as there is already a repricer running..");
                                        }

                                    }
                                }
                                // Remove the started set from the schedule.
                                if (REPRICER_SCHEDULE.containsKey(time)) {
                                    if (regionsToAdd.isEmpty()) {
                                        REPRICER_SCHEDULE.remove(time);
                                        if (SCHEDULE_METADATA.containsKey(time)) {
                                            SCHEDULE_METADATA.remove(time);
                                        }
                                    } else {
                                        REPRICER_SCHEDULE.put(time, regionsToAdd);
                                    }
                                }
                            }
                        }
                    }
                    try {
                        log.info("RepriceScheduler Sleeping for 10 seconds.. " + REPRICER_SCHEDULE);
                        Thread.sleep(10000); // Wait for 10 seconds.
                    } catch (InterruptedException e) {

                    }

                } catch (Exception e) {
                    log.error("Unhandled error in Scheduler", e);
                }

            }

        }
    }

    public synchronized void scheduleRepricer(final List<String> regions, String priority) throws DBException {
        synchronized (REPRICER_SCHEDULE) {
            if (REPRICER_SCHEDULE.size() > 10) {
                log.warn("There are more reprice schedules in the queue, not adding any more schedules");
            }
            long scheduleId = System.currentTimeMillis();
            if ("MANUAL".equals(priority) || "MANUAL_CONTINUE".equals(priority)) {
                List<Long> entryToRemove = new ArrayList<Long>();
                for (Entry<Long, List<String>> entry : REPRICER_SCHEDULE.entrySet()) {
                    List<String> regionsInSchedule = new ArrayList<String>();
                    regionsInSchedule.addAll(entry.getValue());
                    List<String> toRemove = new ArrayList<String>();
                    for (String region : entry.getValue()) {
                        if (regions.contains(region)) {
                            toRemove.add(region);
                            synchronized (MAP_REPRICE_WORKER) {
                                Pair<RepriceWorker, Thread> worker = MAP_REPRICE_WORKER.get(region);
                                if (worker != null) {
                                    // Stop the repricer and remove from the
                                    // list.
                                    if (worker.getSecond() != null && worker.getSecond().isAlive()) {
                                        if (worker.getFirst() != null) {
                                            worker.getFirst().stop(region);
                                        }
                                    }
                                    MAP_REPRICE_WORKER.remove(region);
                                }
                            }
                        }
                    }
                    regionsInSchedule.removeAll(toRemove);
                    entry.setValue(regionsInSchedule);
                    if (regionsInSchedule.isEmpty()) {
                        entryToRemove.add(entry.getKey());
                    }
                }
                // Remove all the unwanted entries..
                for (Long toRemove : entryToRemove) {
                    REPRICER_SCHEDULE.remove(toRemove);
                    synchronized (SCHEDULE_METADATA) {
                        if (SCHEDULE_METADATA.containsKey(toRemove)) {
                            SCHEDULE_METADATA.remove(toRemove);
                        }
                    }
                }
            } else if ("PERIODIC".equals(priority)) {
                for (Entry<Long, List<String>> entry : REPRICER_SCHEDULE.entrySet()) {
                    List<String> regionsInSchedule = entry.getValue();
                    List<String> toRemove = new ArrayList<String>();
                    for (String region : regionsInSchedule) {
                        if (regions.contains(region)) {
                            toRemove.add(region);
                        }
                    }
                    regions.removeAll(toRemove);
                }
            }
            if (!regions.isEmpty()) {
                REPRICER_SCHEDULE.put(scheduleId, regions);
                if ("MANUAL_CONTINUE".equals(priority)) {
                    synchronized (SCHEDULE_METADATA) {
                        SCHEDULE_METADATA.put(scheduleId, "CONTINUE");
                    }
                }
                for (String region : regions) {
                    RepricerConfigurationDAO cfgDAO = new RepricerConfigurationDAO();
                    cfgDAO.setStatus(region, "WAITING", null);
                }
            }
        }
    }

    public void stopRepricer(List<String> regions) throws DBException {
        synchronized (REPRICER_SCHEDULE) {
            log.info("Called StopRepricer for " + regions + " " + REPRICER_SCHEDULE);
            // Stops ongoing repricing.
            for (String region : regions) {
                Pair<RepriceWorker, Thread> worker = MAP_REPRICE_WORKER.get(region);
                if (worker != null) {
                    // Stop the repricer and remove from the list.
                    if (worker.getSecond() != null && worker.getSecond().isAlive()) {
                        if (worker.getFirst() != null) {
                            worker.getFirst().stop(region);
                        }
                    }
                    RepricerConfigurationDAO cfgDAO = new RepricerConfigurationDAO();
                    cfgDAO.setStatus(region, "TERMINATED", null);
                    synchronized (MAP_REPRICE_WORKER) {
                        MAP_REPRICE_WORKER.remove(region);
                    }
                }
            }
            // Stops scheduled repricing.
            List<Long> entryToRemove = new ArrayList<Long>();
            for (Entry<Long, List<String>> entry : REPRICER_SCHEDULE.entrySet()) {
                List<String> regionsInSchedule = new ArrayList<String>();
                regionsInSchedule.addAll(entry.getValue());
                List<String> toRemove = new ArrayList<String>();
                for (String region : entry.getValue()) {
                    if (regions.contains(region)) {
                        RepricerConfigurationDAO cfgDAO = new RepricerConfigurationDAO();
                        cfgDAO.setStatus(region, "TERMINATED", null);
                        toRemove.add(region);
                    }
                }
                regionsInSchedule.removeAll(toRemove);
                entry.setValue(regionsInSchedule);
                if (regionsInSchedule.isEmpty()) {
                    entryToRemove.add(entry.getKey());
                }
            }
            // Remove all the unwanted entries..
            for (Long toRemove : entryToRemove) {
                REPRICER_SCHEDULE.remove(toRemove);
                synchronized (SCHEDULE_METADATA) {
                    if (SCHEDULE_METADATA.containsKey(toRemove)) {
                        SCHEDULE_METADATA.remove(toRemove);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String[] i2 = { "DE" };
        RepricerEngine.getInstance().scheduleRepricer(Arrays.asList(i2), "PERIODIC");
        Thread.sleep(1000);
        String[] i1 = { "DE" };
        RepricerEngine.getInstance().scheduleRepricer(Arrays.asList(i1), "MANUAL");
        Thread.sleep(1000);
        System.out.println(System.currentTimeMillis());
        String[] i3 = { "DE" };
        RepricerEngine.getInstance().scheduleRepricer(Arrays.asList(i3), "MANUAL");
        Thread.sleep(1000);
        String[] regions = { "JP", "US", "UK", "CA" };
        RepricerEngine.getInstance().scheduleRepricer(Arrays.asList(regions), "MANUAL");
        Thread.sleep(1000);
        String[] i4 = { "US", "DE" };
        RepricerEngine.getInstance().stopRepricer(Arrays.asList(i4));
        Thread.sleep(1000);
    }

    public synchronized void pauseRepricer(String region) {
        synchronized (REPRICER_SCHEDULE) {
            if (MAP_REPRICE_WORKER.containsKey(region)) {
                Pair<RepriceWorker, Thread> worker = MAP_REPRICE_WORKER.get(region);
                if (worker.getFirst() != null) {
                    worker.getFirst().pause(region);
                }
                log.info("Paused repricer for the region: " + region);
            }
        }
    }

    public void continueRepricer(String region) throws DBException {
        synchronized (REPRICER_SCHEDULE) {
            List<String> regions = new ArrayList<String>();
            regions.add(region);
            this.scheduleRepricer(regions, "MANUAL_CONTINUE");
        }
    }
}
