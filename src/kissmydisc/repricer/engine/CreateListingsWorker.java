package kissmydisc.repricer.engine;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.utils.Pair;

public class CreateListingsWorker {

    private String region;

    private static final Log log = LogFactory.getLog(CreateListingsWorker.class);

    private static final int NEW = 1;

    private static final int USED = 2;

    private static final int OBI = 3;

    private String fileUrl;

    public CreateListingsWorker(String region) {
        this.region = region;
    }

    public CreateListingsWorker(String region, String fileUrl) {
        this.region = region;
        this.fileUrl = fileUrl;
    }

    public void createListings() throws Exception {
        if (fileUrl == null) {
            createListingsFromDB();
        } else {
            createListingsFromFile();
        }
    }

    protected static String downloadFile(String url, String filePath) throws IOException {
        InputStream is = null;
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(filePath));
        try {
            is = new URL(url).openStream();
            int len;
            byte buf[] = new byte[4096];
            while ((len = is.read(buf)) != -1) {
                os.write(buf, 0, len);
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (os != null) {
                os.close();
            }
        }

        return filePath;
    }

    public void createListingsFromFile() throws Exception {
        String filePath = "downloads/" + System.currentTimeMillis();
        downloadFile(fileUrl, filePath);
        Pair<Long, Pair<Long, Long>> latestInventory = new LatestInventoryDAO()
                .getLatestInventoryWithCountAndId(region);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        Map<String, List<Integer>> availabilityMap = new HashMap<String, List<Integer>>();
        long newInventoryId = System.currentTimeMillis() / 1000;
        int totalAdded = 0;
        try {
            String line = null;
            do {
                line = reader.readLine();
                if (line != null) {
                    String productId = line.trim();
                    List<InventoryFeedItem> items = new InventoryItemDAO().getMatchingItems(latestInventory.getFirst(),
                            region, productId);
                    List<Integer> availability = new ArrayList<Integer>();
                    if (items != null) {
                        for (InventoryFeedItem item : items) {
                            if (item.isValid()) {
                                if (item.getCondition() == 11) {
                                    availability.add(NEW);
                                }
                                if (item.getCondition() < 11) {
                                    if (item.getObiItem()) {
                                        availability.add(OBI);
                                    } else {
                                        availability.add(USED);
                                    }
                                }
                            }
                        }
                    }
                    availabilityMap.put(productId, availability);
                    if (availabilityMap.size() > 500) {
                        totalAdded += createNewListings(newInventoryId, availabilityMap);
                        new LatestInventoryDAO().setLatestInventory("N-" + region, newInventoryId, totalAdded);
                        availabilityMap = new HashMap<String, List<Integer>>();
                    }
                } else {
                    if (availabilityMap != null && availabilityMap.size() > 0) {
                        totalAdded += createNewListings(newInventoryId, availabilityMap);
                        new LatestInventoryDAO().setLatestInventory("N-" + region, newInventoryId, totalAdded);
                        availabilityMap = new HashMap<String, List<Integer>>();
                    }
                }
            } while (line != null);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public void createListingsFromDB() throws Exception {
        InventoryItemDAO inventoryDAO = new InventoryItemDAO();
        int limit = 500;
        String moreToken = null;
        Pair<Long, Pair<Long, Long>> latestInventory = new LatestInventoryDAO()
                .getLatestInventoryWithCountAndId(region);
        String identifiedMoreToken = null;
        String lastProcessed = null;
        long newInventoryId = System.currentTimeMillis() / 1000;
        int totalAdded = 0;
        do {
            Pair<List<InventoryFeedItem>, String> itemsAndMoreToken = inventoryDAO.getMatchingItems(
                    latestInventory.getFirst(), region, moreToken, limit);
            moreToken = itemsAndMoreToken.getSecond();
            List<InventoryFeedItem> items = itemsAndMoreToken.getFirst();
            Map<String, List<Integer>> availabilityMap = new HashMap<String, List<Integer>>();
            for (InventoryFeedItem item : items) {
                if (item.isValid()) {
                    if (!availabilityMap.containsKey(item.getProductId())) {
                        availabilityMap.put(item.getProductId(), new ArrayList<Integer>());
                    }
                    int condition = NEW;
                    if (item.getCondition() < 11) {
                        condition = USED;
                        if (item.getObiItem()) {
                            condition = OBI;
                        }
                    }
                    availabilityMap.get(item.getProductId()).add(condition);
                }
                if (lastProcessed != item.getRegionProductId()) {
                    identifiedMoreToken = lastProcessed;
                }
                lastProcessed = item.getRegionProductId();
            }
            String asin = lastProcessed.split("_")[1];
            if (moreToken != null) {
                availabilityMap.remove(asin);
                moreToken = identifiedMoreToken;
            }
            totalAdded += createNewListings(newInventoryId, availabilityMap);
            new LatestInventoryDAO().setLatestInventory("N-" + region, newInventoryId, totalAdded);
        } while (moreToken != null);
    }

    private int createNewListings(long newInventoryId, Map<String, List<Integer>> availabilityMap) throws DBException {
        int totalAdded = 0;
        for (Map.Entry<String, List<Integer>> entry : availabilityMap.entrySet()) {
            String asin = entry.getKey();
            List<Integer> availableConditions = entry.getValue();
            List<InventoryFeedItem> items = new ArrayList<InventoryFeedItem>();
            InventoryFeedItem item = new InventoryFeedItem();
            item.setInventoryId(newInventoryId);
            item.setObiItem(false);
            item.setPrice(20000.0F);
            item.setQuantity(0);
            item.setProductId(asin);
            item.setRegionProductId("N-" + region + "_" + asin);
            item.setRegion("N-" + region);
            if (!availableConditions.contains(NEW)) {
                item.setCondition(11);
                item.setSku(getRandomString());
                items.add(item);
            }
            if (!availableConditions.contains(USED)) {
                InventoryFeedItem newItem = clone(item);
                newItem.setCondition(2);
                newItem.setSku(getRandomString());
                items.add(newItem);
            }
            if (!availableConditions.contains(OBI)) {
                InventoryFeedItem newItem = clone(item);
                newItem.setObiItem(true);
                newItem.setCondition(2);
                newItem.setSku(getRandomString());
                items.add(newItem);
            }
            totalAdded += items.size();
            if (items.size() > 0) {
                new InventoryItemDAO().addItems(items);
            }
        }
        return totalAdded;
    }

    private InventoryFeedItem clone(InventoryFeedItem item) {
        InventoryFeedItem newItem = new InventoryFeedItem();
        newItem.setCondition(item.getCondition());
        newItem.setInventoryId(item.getInventoryId());
        newItem.setInventoryItemId(item.getInventoryItemId());
        newItem.setItemNo(item.getItemNo());
        newItem.setLowestAmazonPrice(item.getLowestAmazonPrice());
        newItem.setObiItem(item.getObiItem());
        newItem.setPrice(item.getPrice());
        newItem.setProductId(item.getProductId());
        newItem.setQuantity(item.getQuantity());
        newItem.setRegion(item.getRegion());
        newItem.setRegionProductId(item.getRegionProductId());
        newItem.setSku(item.getSku());
        newItem.setWeight(item.getWeight());
        return newItem;
    }

    private String getRandomString() {
        return UUID.randomUUID().toString();
    }
}
