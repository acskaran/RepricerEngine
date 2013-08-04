package kissmydisc.repricer.engine;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.congrace.exp4j.Calculable;

import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.CurrencyConversionDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.dao.ProductDAO;
import kissmydisc.repricer.dao.RepriceReportDAO;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.dao.RepricerStatusReportDAO;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.PriceQuantityFeed;
import kissmydisc.repricer.model.ProductDetails;
import kissmydisc.repricer.model.RepriceReport;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.model.RepricerFormula;
import kissmydisc.repricer.model.RepricerStatus;
import kissmydisc.repricer.utils.AppConfig;
import kissmydisc.repricer.utils.Pair;

public class RepriceWorker implements Runnable {

    public String region;

    private static final String COMPLETED = "COMPLETED";

    private static final String TERMINATED = "TERMINATED";

    public Map<String, Boolean> regionsToIgnore = new Hashtable<String, Boolean>();

    private static final Log log = LogFactory.getLog(RepriceWorker.class);

    private static final int MAX_REQUESTS = 20;

    private static final int BATCH_SIZE = AppConfig.getInteger("AMAZON_BATCH_SIZE", 10);

    private static final int THREADS = (MAX_REQUESTS / BATCH_SIZE) + ((MAX_REQUESTS % BATCH_SIZE == 0) ? 0 : 1);

    private String metadata;

    private boolean shouldContinue = false;

    public RepriceWorker(String region, String metadata) {
        this.region = region;
        this.metadata = metadata; // Check if CONTINUE and continue;
        if (this.metadata != null) {
            if (this.metadata.equals("CONTINUE")) {
                shouldContinue = true;
                // reinitialize();
            }
        }
    }

    private ThreadPoolExecutor executor;

    private BlockingQueue<Runnable> executorQueue = new ArrayBlockingQueue<Runnable>(2000);

    private RepriceFeedManager feedManager;

    List<RepricerConfiguration> repriceConfigs;

    private RepricerStatus status = null;

    private AmazonAccessor amazonAccessor = null;

    private AmazonAccessor jpAccessor = null;

    private Map<String, AmazonAccessor> amazonAccessorMap = new HashMap<String, AmazonAccessor>();

    private RepricerStatusReportDAO rsrdao = new RepricerStatusReportDAO();

    private Map<String, Float> exchangeRates;

    private boolean paused = false;

    public void run() {
        log.info("Started Repricing for " + region);
        RepricerConfigurationDAO configDAO = new RepricerConfigurationDAO();
        executor = new ThreadPoolExecutor(THREADS, THREADS, 100, TimeUnit.SECONDS, executorQueue);
        rsrdao = new RepricerStatusReportDAO();
        try {
            repriceConfigs = configDAO.getRepricers();
            log.info(repriceConfigs);
            RepricerConfiguration JPConfig = null;
            exchangeRates = new CurrencyConversionDAO().getCurrencyConversion();
            for (RepricerConfiguration config : repriceConfigs) {
                AmazonAccessor aAcc = new AmazonAccessor(config.getRegion(), config.getMarketplaceId(),
                        config.getSellerId());
                amazonAccessorMap.put(config.getRegion(), aAcc);
                if ("JP".equals(config.getRegion())) {
                    amazonAccessor = aAcc;
                }
            }

            jpAccessor = amazonAccessor;

            log.info("Running repricer for " + region);
            executorQueue = new ArrayBlockingQueue<Runnable>(2000);
            executor = new ThreadPoolExecutor(THREADS, THREADS, 100, TimeUnit.SECONDS, executorQueue);
            try {
                RepricerConfiguration currentConfig = null;
                for (RepricerConfiguration config : repriceConfigs) {
                    if (config.getRegion().equals(region)) {
                        currentConfig = config;
                    }
                }
                if (currentConfig != null) {
                    if (shouldContinue) {
                        status = rsrdao.getRepricerStatus(currentConfig.getRegion());
                        status.setStatus("RUNNING");
                        log.info("Continuing from where we left." + status);
                    } else {
                        status = rsrdao.addNewRepricer(currentConfig.getRegion());
                    }
                    amazonAccessor = new AmazonAccessor(currentConfig.getRegion(), currentConfig.getMarketplaceId(),
                            currentConfig.getSellerId());
                    if ("KMD".equals(currentConfig.getRegion())) {
                        feedManager = new KMDRepriceFeedManager(status.getRepriceId());
                    } else {
                        feedManager = new AmazonRepriceFeedManager(status.getRepriceId(), currentConfig.getRegion(),
                                amazonAccessor);
                    }
                    configDAO.setStatus(currentConfig.getRegion(), "RUNNING", status.getRepriceId());
                    int retVal = processRegion(currentConfig);
                    feedManager.flush();
                    feedManager.close();
                    executor.shutdown();
                    if (retVal == 0) {
                        if (currentConfig.getInterval() < 0) {
                            configDAO.setCompletedOrTerminated(region, COMPLETED);
                            status.setStatus(COMPLETED);
                            rsrdao.updateStatus(status);
                        } else {
                            configDAO.setNextRun(region, "SCHEDULED",
                                    System.currentTimeMillis() + currentConfig.getInterval());
                            status.setStatus(COMPLETED);
                            rsrdao.updateStatus(status);
                        }
                    } else if (retVal == -1) {
                        configDAO.setCompletedOrTerminated(region, TERMINATED);
                        status.setStatus(TERMINATED);
                        rsrdao.updateStatus(status);
                    } else if (retVal == -2) {
                        configDAO.setCompletedOrTerminated(region, "PAUSED");
                        status.setStatus("PAUSED");
                        rsrdao.updateStatus(status);
                    }
                }
            } catch (Exception e) {
                log.error("Error while repricing the region.." + region, e);
                try {
                    configDAO.setStatus(region, "ERROR", null);
                    status.setStatus("ERROR");
                    rsrdao.updateStatus(status);
                    executor.shutdown();
                } catch (DBException e1) {
                    // Nothing to do here.
                }
            } finally {
                try {
                    feedManager.flush();
                } catch (Exception e) {
                    log.error("Error flushing data to Amazon.", e);
                }
                feedManager.close();
                if (executor != null) {
                    executor.shutdownNow();
                }
            }
        } catch (Exception e) {
            log.error("Error while repricing.", e);
            try {
                configDAO.setCompletedOrTerminated(region, "ERROR");
            } catch (DBException e1) {
                log.error("Error updating status to error.", e);
            }
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }
            if (feedManager != null) {
                try {
                    feedManager.flush();
                } catch (Exception e) {
                    log.error("Error flushing data.", e);
                }
                try {
                    feedManager.close();
                } catch (Exception e) {

                }
            }
        }

    }

    private int processRegion(RepricerConfiguration currentConfig) throws DBException {
        String region = currentConfig.getRegion();
        long cacheInterval = currentConfig.getCacheRefreshTime();
        LatestInventoryDAO latestInventoryDAO = new LatestInventoryDAO();
        long latestInventory = latestInventoryDAO.getLatestInventory(region);
        InventoryItemDAO inventoryDAO = new InventoryItemDAO();
        int limit = 500;
        String moreToken = status.getLastRepriced();
        status.setTotalScheduled((int) status.getLastRepricedId());
        status.setTotalCompleted((int) status.getLastRepricedId());
        int submittedTasks = 0;
        long itemNo = status.getLastRepricedId() + 1;
        do {
            Pair<List<InventoryFeedItem>, String> itemsAndMoreToken = inventoryDAO.getMatchingItems(latestInventory,
                    currentConfig.getRegion(), moreToken, limit);
            List<InventoryFeedItem> items = itemsAndMoreToken.getFirst();
            moreToken = itemsAndMoreToken.getSecond();
            log.debug("Repricer " + region + " got " + itemsAndMoreToken.getFirst().size() + " to process, moreToken: "
                    + moreToken);
            if (items != null) {
                List<InventoryFeedItem> toProcessCached = new ArrayList<InventoryFeedItem>();
                List<InventoryFeedItem> toProcessAndCache = new ArrayList<InventoryFeedItem>();
                for (int i = 0; i < items.size(); i += BATCH_SIZE) {
                    int toIndex = Math.min(i + BATCH_SIZE, items.size());
                    List<InventoryFeedItem> toProcess = items.subList(i, toIndex);
                    List<String> productIds = new ArrayList<String>();
                    for (InventoryFeedItem item : toProcess) {
                        productIds.add(item.getProductId());
                    }
                    Map<String, ProductDetails> details = new ProductDAO().getProductDetails(productIds);
                    for (InventoryFeedItem item : toProcess) {
                        boolean shouldCache = true;
                        if (details != null && details.containsKey(item.getProductId())) {
                            ProductDetails detail = details.get(item.getProductId());
                            if (detail.getLastUpdated() != null) {
                                if (cacheInterval >= 0
                                        && detail.getLastUpdated().after(
                                                new Date(System.currentTimeMillis() - cacheInterval))) {
                                    shouldCache = false;
                                }
                            }
                        }
                        List<InventoryFeedItem> toProcessTemp = null;
                        item.setItemNo(itemNo++);
                        if (shouldCache) {
                            toProcessAndCache.add(item);
                            toProcessTemp = toProcessAndCache;
                        } else {
                            toProcessCached.add(item);
                            toProcessTemp = toProcessCached;
                        }
                        if (toProcessTemp.size() == BATCH_SIZE) {
                            status.addScheduled(toProcessTemp.size());
                            executor.execute(new WorkerThreadInternational(toProcessTemp, currentConfig, feedManager));
                            submittedTasks++;
                            if (shouldCache) {
                                toProcessAndCache = new ArrayList<InventoryFeedItem>();
                            } else {
                                toProcessCached = new ArrayList<InventoryFeedItem>();
                            }
                        }
                    }
                }
                if (toProcessCached != null && !toProcessCached.isEmpty()) {
                    status.addScheduled(toProcessCached.size());
                    executor.execute(new WorkerThreadInternational(toProcessCached, currentConfig, feedManager));
                    submittedTasks++;
                }
                if (toProcessAndCache != null && !toProcessAndCache.isEmpty()) {
                    status.addScheduled(toProcessAndCache.size());
                    executor.execute(new WorkerThreadInternational(toProcessAndCache, currentConfig, feedManager));
                    submittedTasks++;
                }
            }

            rsrdao.updateStatus(status);

            while (!isTerminated(region) && !paused && executor.getQueue().size() > 1000) {
                try {
                    Thread.sleep(10000);
                    rsrdao.updateStatus(status);
                } catch (InterruptedException e) {
                    // Nothing to do here.
                }
            }

        } while (moreToken != null && !isTerminated(region) && !paused);
        // Wait for it to complete.
        while (!isTerminated(region) && !paused && executor.getCompletedTaskCount() < submittedTasks) {
            try {
                log.debug("Repricer: " + region + "Sleeping for 10 seconds - completed: "
                        + executor.getCompletedTaskCount() + ", submitted: " + submittedTasks);
                Thread.sleep(10000);
                rsrdao.updateStatus(status);
            } catch (InterruptedException e) {
                // Nothing to do here.
            }
        }
        if (isTerminated(region) || paused) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
            }
            if (paused) {
                log.info("Paused " + region);
                return -2;
            } else {
                log.info("Terminated " + region);
                return -1;
            }
        }
        executor.shutdown();
        return 0;
    }

    private boolean isTerminated(String region) {
        synchronized (regionsToIgnore) {
            if (regionsToIgnore.containsKey(region)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private RepriceReport toRepriceReport(long inventoryItemId, int formulaId, PriceQuantityFeed feed,
            StringBuffer auditTrail) {
        RepriceReport report = new RepriceReport();
        report.setRepriceId(status.getRepriceId());
        report.setFormulaId(formulaId);
        report.setInventoryItemId(inventoryItemId);
        report.setPrice(feed.getPrice());
        report.setQuantity(feed.getQuantity());
        report.setAuditTrail(auditTrail.toString());
        return report;
    }

    private class WorkerThreadInternational implements Runnable {

        private List<InventoryFeedItem> items;

        private RepricerConfiguration config;

        private RepriceFeedManager feedManager;

        private int index;

        public WorkerThreadInternational(final List<InventoryFeedItem> items, final RepricerConfiguration config,
                final RepriceFeedManager feedManager) {
            this.items = items;
            this.config = config;
            this.feedManager = feedManager;
        }

        @Override
        public void run() {

            ExternalDataManager dataManager = null;

            if (this.config.getRegion().equals("KMD")) {
                dataManager = new ExternalDataManager(this.items, this.config.getCacheRefreshTime(), jpAccessor,
                        amazonAccessorMap, exchangeRates);
            } else {
                dataManager = new ExternalDataManager(this.items, this.config.getCacheRefreshTime(), jpAccessor,
                        amazonAccessor);
            }

            try {

                List<RepriceReport> reports = new ArrayList<RepriceReport>();

                for (InventoryFeedItem item : items) {
                    StringBuffer auditTrail = new StringBuffer();
                    PriceQuantityFeed feed = new PriceQuantityFeed(item.getRegion());
                    feed.setSku(item.getSku());
                    boolean reprice = true;
                    double price = (double) item.getPrice();
                    int quantity = item.getQuantity();
                    auditTrail.append("P:" + item.getPrice() + ", Q:" + quantity + "\n");
                    // Quantity Filter

                    // Quantity Filter
                    index++;
                    boolean quantityReset = false;
                    boolean priceDown = false;
                    boolean priceUp = false;
                    boolean lowestPrice = false;
                    boolean blackList = false;
                    boolean noPriceChange = false;

                    try {
                        if (reprice && dataManager.isBlacklist(item.getSku())) {
                            reprice = false;
                            blackList = true;
                            auditTrail.append("Blacklisted.");
                        }

                        int ajpQuantity = dataManager.getQuantity(item.getSku());
                        double currentPrice = item.getPrice();
                        int currentQuantity = item.getQuantity();

                        int qtyLimit = (item.getObiItem() ? config.getFormula().getObiQuantityLimit() : config
                                .getFormula().getQuantityLimit());

                        if (reprice && ajpQuantity >= 0 && ajpQuantity < qtyLimit) {
                            reprice = false;
                            quantity = 0;
                            quantityReset = true;
                            auditTrail.append("Applying Quantity Filter - Ajp Q:" + ajpQuantity + ", Not Repricing.");
                            if (log.isDebugEnabled()) {
                                log.debug("Applying quantity filter for " + item);
                            }
                        } else if (!item.getRegion().equals("JP") && reprice && ajpQuantity >= qtyLimit
                                && currentQuantity == 0) {
                            quantity = 1;
                        }

                        if (reprice) {
                            if (log.isDebugEnabled()) {
                                log.debug("Repricing " + item);
                            }
                            String formula = (item.getObiItem() ? config.getFormula().getObiFormula() : config
                                    .getFormula().getInitialFormula());
                            Float lowestAmazonPrice = dataManager.getLowestAmazonPrice(item.getSku());
                            Float weight = dataManager.getWeight(item.getSku());
                            if (weight == null || weight <= 0F) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Weight is empty, using default weight for " + item.getSku());
                                }
                                weight = (float) (item.getObiItem() ? config.getFormula().getDefaultObiWeight()
                                        : config.getFormula().getDefaultWeight());
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("SKU: " + item.getSku() + ", Region: " + item.getRegion()
                                        + ", Formula used: " + formula + ", LAP: " + lowestAmazonPrice + ", WGT: "
                                        + weight);
                            }
                            Calculable expression;
                            String error = "";
                            try {
                                de.congrace.exp4j.ExpressionBuilder builder = new de.congrace.exp4j.ExpressionBuilder(
                                        formula);
                                if (lowestAmazonPrice != null && lowestAmazonPrice > 0F) {
                                    builder.withVariable("LAP", lowestAmazonPrice);
                                    auditTrail.append("LAP:" + lowestAmazonPrice + "\n");
                                } else {
                                    auditTrail.append("LAP unavailable.");
                                    error += "LAP unavailable,";
                                }
                                if (weight != null && weight >= 0F) {
                                    builder.withVariable("WGT", weight);
                                    auditTrail.append("WGT:" + weight + "\n");
                                } else {
                                    auditTrail.append("WGT unavailable.");
                                    error += "WGT unavailable";
                                }
                                expression = builder.build();
                                price = expression.calculate();
                                auditTrail.append("MyPrice:" + price + "\n");
                                if (currentPrice < price) {
                                    priceUp = true;
                                } else if (currentPrice > price) {
                                    priceDown = true;
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Calculated price and existing prices are the same for SKU: "
                                                + item.getSku() + ", Region: " + item.getRegion());
                                    }
                                    noPriceChange = true;
                                }
                            } catch (Exception e) {
                                log.error("Unable to evaluate the expression " + formula + " for SKU: " + item.getSku()
                                        + ", Region: " + item.getRegion() + ", ErrorString=" + error, e);
                                reprice = false;
                                noPriceChange = true;
                                price = item.getPrice();
                                quantity = item.getQuantity();
                                auditTrail.append("Not repricing.");
                            }

                            // Second level repricing..
                            if (reprice && !item.getObiItem() && config.getFormula().isSecondLevelRepricing()) {
                                Float lowestRegionalPrice = dataManager.getLowestRegionalPrice(item.getSku());
                                if (lowestRegionalPrice != null && lowestRegionalPrice > 0) {
                                    RepricerFormula f = config.getFormula();
                                    if (price < lowestRegionalPrice) {
                                        double upperLimit = f.getSecondLevelRepricingUpperLimit();
                                        double upperLimitPercent = f.getSecondLevelRepricingUpperLimitPercent();
                                        double diff = Math.min(price * upperLimitPercent / 100, upperLimit);
                                        auditTrail.append("UL:" + diff + ",");
                                        price = Math.min(price + diff, lowestRegionalPrice - f.getLowerPriceMarigin());
                                        auditTrail.append("Price after second level repricing: " + price
                                                + ", LowestPrice: " + lowestRegionalPrice + "\nUp\n");
                                    } else {
                                        double lowerLimit = f.getSecondLevelRepricingLowerLimit();
                                        double lowerLimitPercent = f.getSecondLevelRepricingLowerLimitPercent();
                                        double diff = Math.min(price * lowerLimitPercent / 100, lowerLimit);
                                        auditTrail.append("LL:" + diff + ",");
                                        price = Math.max(price - diff, lowestRegionalPrice - f.getLowerPriceMarigin());
                                        auditTrail.append("Price after second level repricing: " + price
                                                + ", LowestPrice: " + lowestRegionalPrice + "\nPrice Down\n");
                                    }
                                    if (currentPrice < price) {
                                        noPriceChange = false;
                                        priceUp = true;
                                        priceDown = false;
                                    } else if (currentPrice > price) {
                                        noPriceChange = false;
                                        priceDown = true;
                                        priceUp = false;
                                    } else {
                                        noPriceChange = true;
                                        priceUp = false;
                                        priceDown = false;
                                    }
                                    if (lowestRegionalPrice > price) {
                                        lowestPrice = true;
                                    }
                                } else {
                                    if (lowestRegionalPrice != null && lowestRegionalPrice < 0) {
                                        RepricerFormula f = config.getFormula();
                                        double upperLimit = f.getSecondLevelRepricingUpperLimit();
                                        double upperLimitPercent = f.getSecondLevelRepricingUpperLimitPercent();
                                        double diff = Math.min(price * upperLimitPercent / 100, upperLimit);
                                        auditTrail.append("UL:" + diff + ",");
                                        price = price + diff;
                                        auditTrail
                                                .append("Price after applying upper limit: " + price + "\nPrice Up\n");
                                    } else {
                                        auditTrail.append(item.getRegion()
                                                + " price not available. Skipping second level repricing");
                                    }
                                }
                            }

                            feed.setPrice((float) price);
                            feed.setQuantity(quantity);
                            if (log.isDebugEnabled()) {
                                log.debug(feed);
                            }
                            reports.add(toRepriceReport(item.getInventoryItemId(), this.config.getFormula()
                                    .getFormulaId(), feed, auditTrail));
                            RepricerStatus.METRIC metric = RepricerStatus.METRIC.SAME_PRICE;
                            if (item.getObiItem()) {
                                metric = RepricerStatus.METRIC.OBI_SAME_PRICE;
                            }
                            if (quantityReset) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.QUANTITY_RESET_TO_ZERO;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_QUANTITY_RESET_TO_ZERO;
                                }
                            } else if (priceUp) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.PRICE_UP;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_PRICE_UP;
                                }
                            } else if (priceDown) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.PRICE_DOWN;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_PRICE_DOWN;
                                }
                            }
                            try {
                                if (!noPriceChange) {
                                    feedManager.writeToFeedFile(feed);
                                }
                            } catch (Exception e) {
                                log.error("Error writing data to Amazon.", e);
                            }
                            status.addRepriceMetrics(item.getRegionProductId(), item.getItemNo(), metric, lowestPrice);
                        } else {
                            feed.setQuantity(quantity);
                            feed.setPrice((float) price);
                            if (log.isDebugEnabled()) {
                                log.debug(feed);
                            }
                            reports.add(toRepriceReport(item.getInventoryItemId(), this.config.getFormula()
                                    .getFormulaId(), feed, auditTrail));
                            RepricerStatus.METRIC metric = RepricerStatus.METRIC.SAME_PRICE;
                            if (item.getObiItem()) {
                                metric = RepricerStatus.METRIC.OBI_SAME_PRICE;
                            }
                            if (quantityReset) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.QUANTITY_RESET_TO_ZERO;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_QUANTITY_RESET_TO_ZERO;
                                }
                            } else if (priceUp) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.PRICE_UP;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_PRICE_UP;
                                }
                            } else if (priceDown) {
                                if (!item.getObiItem()) {
                                    metric = RepricerStatus.METRIC.PRICE_DOWN;
                                } else {
                                    metric = RepricerStatus.METRIC.OBI_PRICE_DOWN;
                                }
                            }
                            try {
                                if (!metric.equals(RepricerStatus.METRIC.SAME_PRICE)) {
                                    feedManager.writeToFeedFile(feed);
                                }
                            } catch (Exception e) {
                                log.error("Error writing data to Amazon.", e);
                            }
                            status.addRepriceMetrics(item.getRegionProductId(), index, metric, lowestPrice);
                        }
                        if (quantity >= 0 && price > 0) {
                            InventoryItemDAO dao = new InventoryItemDAO();
                            dao.updatePriceAndQuantity(item.getInventoryItemId(), price, quantity, currentPrice,
                                    currentQuantity);
                        }
                    } catch (Exception e) {
                        log.error("Error while repricing the item.", e);
                    }
                    status.addOneCompleted();
                }
                if (reports != null && reports.size() > 0) {
                    // Adding reprice reports
                    RepriceReportDAO dao = new RepriceReportDAO();
                    dao.addItems(reports);
                }

            } catch (DBException e) {
                log.error("Error getting required data from DB", e);
            }
        }
    }

    public void stop(String region) {
        regionsToIgnore.put(region, true);
        log.info("Stopped region." + region);
    }

    public void pause(String region) {
        paused = true;
    }

    public void continueRepricer(String region) {
        paused = false;
    }

}
