package kissmydisc.repricer.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.engine.AmazonRepriceFeedManager;
import kissmydisc.repricer.engine.RepriceFeedManager;
import kissmydisc.repricer.feeds.PriceQuantityFeed;
import kissmydisc.repricer.model.InvalidItem;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.utils.Pair;

public class InvalidateCommand extends Command {

    private String region;

    private String invalidateFile;

    private long inventoryId;

    private RepriceFeedManager feedManager = null;

    private static final Log log = LogFactory.getLog(InvalidateCommand.class);

    public InvalidateCommand(int id, String metadata, Date date) {
        super(id, date);
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("Region=")) {
                region = md.substring("Region=".length());
            }
            if (md.startsWith("InvalidateFile=")) {
                invalidateFile = md.substring("InvalidateFile=".length());
            }
        }
    }

    @Override
    public void execute() {
        CommandDAO dao = new CommandDAO();
        try {
            dao.setStatus(getCommandId(), "STARTED");
            RepricerConfiguration config = new RepricerConfigurationDAO().getRepricer(region);
            AmazonAccessor amazonAccessor = new AmazonAccessor(region, config.getMarketplaceId(), config.getSellerId());
            feedManager = new AmazonRepriceFeedManager(getCommandId(), region, amazonAccessor, false);
            if (region == null || invalidateFile == null) {
                log.info("Invalid Invalidate Command Command, " + this);
            } else {
                String filePath = downloads + File.separator + "InvalidateCommandTmp-" + System.currentTimeMillis();
                String downloadedFile = downloadFile(invalidateFile, filePath);
                inventoryId = new LatestInventoryDAO().getLatestInventory(region);
                invalidate(downloadedFile);
            }
            dao.setStatus(getCommandId(), "COMPLETED");
        } catch (Exception e) {
            log.error("Error blacklisting", e);
            try {
                dao.setStatus(getCommandId(), "ERROR");
            } catch (DBException e1) {
            }
        }
    }

    private void invalidate(String file) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        Map<Integer, String> invalidHeader = new HashMap<Integer, String>();
        try {
            int count = 0;
            String line = null;
            String header = reader.readLine();
            if (header != null) {
                header = header.trim();
                String[] headerParts = header.split("\t");
                for (int i = 0; i < headerParts.length; i++) {
                    invalidHeader.put(i, headerParts[i]);
                }
                do {
                    line = reader.readLine();
                    if (line != null) {
                        InvalidItem invalidItem = new InvalidItem();
                        invalidItem.setRegion(region);
                        boolean isValid = false;
                        String[] parts = line.split("\t");
                        for (int i = 0; i < parts.length; i++) {
                            if (invalidHeader.containsKey(i)) {
                                String key = invalidHeader.get(i);
                                if (key.equals("sku")) {
                                    invalidItem.setSku(parts[i]);
                                    isValid = true;
                                }
                                if (key.equals("product-id")) {
                                    invalidItem.setProductId(parts[i]);
                                    isValid = true;
                                }
                                if (key.equals("item-condition")) {
                                    try {
                                        invalidItem.setItemCondition(Integer.parseInt(parts[i]));
                                    } catch (Exception e) {
                                        log.warn("Invalid item condition for invalidate request " + line);
                                        isValid = false;
                                    }
                                }
                            }
                        }
                        if (isValid) {
                            count += invalidate(invalidItem);
                        }
                    }
                } while (line != null);
            }
            Pair<Long, Pair<Long, Long>> invCount = new LatestInventoryDAO().getLatestInventoryWithCountAndId(region);
            new LatestInventoryDAO().setLatestInventory(region, invCount.getFirst(), invCount.getSecond().getSecond()
                    - count);
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (feedManager != null) {
                feedManager.flush();
            }
        }
    }

    private int invalidate(InvalidItem item) throws Exception {
        InventoryItemDAO iidao = new InventoryItemDAO();
        List<InventoryFeedItem> items = iidao.getMatchingItems(inventoryId, region, item);
        int count = 0;
        if (items != null && items.size() != 0) {
            for (InventoryFeedItem feedItem : items) {
                if (feedItem.isValid()) {
                    PriceQuantityFeed feed = new PriceQuantityFeed(region);
                    feedItem.setValid(false);
                    feedItem.setQuantity(0);
                    feed.setPrice(feedItem.getPrice());
                    feed.setQuantity(0);
                    feed.setSku(feedItem.getSku());
                    feedManager.writeToFeedFile(feed);
                    count++;
                }
            }
            iidao.invaliate(items);
        }
        return count;
    }
}
