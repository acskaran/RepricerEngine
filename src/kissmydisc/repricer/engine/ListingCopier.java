package kissmydisc.repricer.engine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.RepricerMainThreadPool;
import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.FeedsDAO;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.dao.ListingConfigurationDAO;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.feeds.PriceQuantityFeed;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.InventoryLoaderConfiguration;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.utils.AppConfig;
import kissmydisc.repricer.utils.Pair;
import kissmydisc.repricer.utils.PriceUtils;

public class ListingCopier {

    private String fromRegion;

    private String toRegion;

    public ListingCopier(String fromRegion, String toRegion) {
        this.fromRegion = fromRegion;
        this.toRegion = toRegion;
    }

    private List<String> submissionIds = new ArrayList<String>();

    private List<Pair<String, String>> pqFiles = new ArrayList<Pair<String, String>>();

    private static String TAB = "\t";

    private static String NEWLINE = "\n";

    private DigestOutputStream dos = null;

    private int byteWritten = 0;

    private String directory = "feeds/";

    private String feedFile;

    private DigestOutputStream pqFileStream;

    private String pqFeedFile;

    private int rowsWritten = 0;

    private static final Log log = LogFactory.getLog(ListingCopier.class);

    public void copyListings() throws Exception {
        InventoryItemDAO inventoryDAO = new InventoryItemDAO();
        int limit = 100;
        String moreToken = null;
        Pair<Long, Pair<Long, Long>> latestInventory = new LatestInventoryDAO()
                .getLatestInventoryWithCountAndId(fromRegion);
        int totalAdded = 0;
        String identifiedMoreToken = null;
        String lastProcessed = null;
        Pair<Long, Pair<Long, Long>> inventoryDetails = new LatestInventoryDAO()
                .getLatestInventoryWithCountAndId(toRegion);
        long toInventoryId = inventoryDetails.getFirst();
        long totalItems = inventoryDetails.getSecond().getSecond();
        long id = inventoryDetails.getSecond().getFirst() + 1;
        InventoryLoaderConfiguration config = new ListingConfigurationDAO().getListingConfiguration(toRegion);
        // Adding other details
        config.setQuantity(1);
        config.setAddDelete("a");
        Map<String, Boolean> doneLastIteration = new HashMap<String, Boolean>();
        try {
            do {
                Map<String, Boolean> doneThisIteration = new HashMap<String, Boolean>();
                Pair<List<InventoryFeedItem>, String> itemsAndMoreToken = inventoryDAO.getMatchingItems(
                        latestInventory.getFirst(), fromRegion, moreToken, limit);
                moreToken = itemsAndMoreToken.getSecond();
                List<InventoryFeedItem> items = itemsAndMoreToken.getFirst();
                List<InventoryFeedItem> newItems = new ArrayList<InventoryFeedItem>();
                List<PriceQuantityFeed> quantityZero = new ArrayList<PriceQuantityFeed>();
                for (InventoryFeedItem item : items) {
                    if (lastProcessed != item.getRegionProductId()) {
                        identifiedMoreToken = lastProcessed;
                    }
                    String key = item.getProductId() + " " + item.getCondition() + " " + item.getObiItem();
                    lastProcessed = item.getRegionProductId();
                    PriceQuantityFeed pqFeed = new PriceQuantityFeed(toRegion);
                    if (!doneLastIteration.containsKey(key) && !doneThisIteration.containsKey(key)) {
                        item.setInventoryId(toInventoryId);
                        String sku = item.getSku();
                        if (item.getCondition() == 11) {
                            sku = "N-MA-" + toRegion + id++;
                        }
                        if (item.getCondition() == 2) {
                            if (item.getObiItem()) {
                                sku = "O-MA-" + toRegion + id++;
                            } else {
                                sku = "U-MA-" + toRegion + id++;
                            }
                        }
                        pqFeed.setPrice(item.getPrice());
                        pqFeed.setQuantity(item.getQuantity());
                        if (item.getQuantity() == 0) {
                            pqFeed.setPrice(20000);
                            quantityZero.add(pqFeed);
                        }
                        item.setSku(sku);
                        pqFeed.setSku(sku);
                        item.setRegion(toRegion);
                        item.setRegionProductId(toRegion + "_" + item.getProductId());
                        newItems.add(item);
                    }
                    doneThisIteration.put(key, true);
                }
                doneLastIteration = doneThisIteration;
                totalAdded += newItems.size();
                new LatestInventoryDAO().updateLatestInventory(toRegion, toInventoryId, totalItems, id);
                new InventoryItemDAO().addItems(newItems);
                new LatestInventoryDAO().setLatestInventory(toRegion, toInventoryId, totalItems + totalAdded);
                buffer(toRegion, newItems, config);
                bufferPriceQuantity(toRegion, quantityZero);
                if (moreToken != null) {
                    moreToken = identifiedMoreToken;
                }
            } while (moreToken != null);
        } finally {
            if (dos != null) {
                byte[] md5 = dos.getMessageDigest().digest();
                dos.flush();
                dos.close();
                byteWritten = 0;
                dos = null;
                submitToAmazon(feedFile, md5);
            }
            if (pqFileStream != null) {
                pqFileStream.flush();
                byte[] md5 = pqFileStream.getMessageDigest().digest();
                pqFileStream.close();
                rowsWritten = 0;
                pqFileStream = null;
                pqFiles.add(new Pair<String, String>(pqFeedFile, new String(Base64.encodeBase64(md5), "UTF-8")));
            }
            submitToAmazon();
        }
    }

    private void submitToAmazon() throws Exception {
        boolean completed = false;
        int consecutive = 0;
        RepricerConfiguration config = new RepricerConfigurationDAO().getRepricer(toRegion);
        AmazonAccessor amazonAccessor = new AmazonAccessor(toRegion, config.getMarketplaceId(), config.getSellerId());
        do {
            try {
                completed = amazonAccessor.isSubmissionProcessed(submissionIds);
                consecutive = 0;
            } catch (Exception e) {
                consecutive++;
                if (consecutive > 100) {
                    throw new Exception("Consecutive errors while trying to submit feed to Amazon." + pqFiles, e);
                }
                log.error("Error checking submission", e);
            }
            log.info("Waiting for " + submissionIds + " to complete.");
            Thread.sleep(45000);
        } while (completed == false);
        for (Pair<String, String> pq : pqFiles) {
            int retry = 0;
            completed = false;
            do {
                try {
                    log.info("Submitting feed with quantity " + pq.getFirst());
                    amazonAccessor.sendFeed(pq.getFirst(), pq.getSecond());
                    log.info("Submitted feed with quantity " + pq.getFirst());
                    completed = true;
                } catch (Exception e) {
                    log.error("Unable to send feed to Amazon " + pq, e);
                }
            } while (!completed && retry++ < 3);
        }
    }

    private void bufferPriceQuantity(String region, List<PriceQuantityFeed> quantityZero) throws Exception {
        if (pqFileStream == null) {
            pqFeedFile = directory + "QuantityZero-PriceQuantityFeed-" + toRegion + "-" + System.currentTimeMillis();
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(pqFeedFile));
            pqFileStream = new DigestOutputStream(bos, MessageDigest.getInstance("MD5"));
            byte[] header = (PriceQuantityFeed.getHeader() + "\n").getBytes();
            pqFileStream.write(header);
            pqFileStream.flush();
            rowsWritten = 1;
        }
        for (PriceQuantityFeed item : quantityZero) {
            String row = item.toString() + "\n";
            pqFileStream.write(row.getBytes());
            rowsWritten++;
        }
        pqFileStream.flush();
        if (rowsWritten > 50000) {
            byte[] md5 = pqFileStream.getMessageDigest().digest();
            pqFileStream.close();
            rowsWritten = 0;
            pqFileStream = null;
            pqFiles.add(new Pair<String, String>(pqFeedFile, new String(Base64.encodeBase64(md5), "UTF-8")));
        }
    }

    private void buffer(String toRegion, List<InventoryFeedItem> items, InventoryLoaderConfiguration config)
            throws Exception {
        if (dos == null) {
            feedFile = directory + "InventoryLoader-" + toRegion + "-" + System.currentTimeMillis();
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(feedFile));
            dos = new DigestOutputStream(bos, MessageDigest.getInstance("MD5"));
            byte[] header = (config.getHeader() + "\n").getBytes();
            dos.write(header);
            dos.flush();
            byteWritten += header.length;
        }
        for (InventoryFeedItem item : items) {
            int parameter = 1;
            String listing = "";
            while (true) {
                String param = config.getNext(parameter++);
                if (param == null)
                    break;
                if (param.equals("sku")) {
                    listing += item.getSku() + TAB;
                }
                if (param.equals("product-id")) {
                    listing += item.getProductId() + TAB;
                }
                if (param.equals("product-id-type")) {
                    listing += "1" + TAB;
                }
                if (param.equals("item-condition")) {
                    listing += item.getCondition() + TAB;
                }
                if (param.equals("price")) {
                    listing += PriceUtils.getPrice(item.getPrice(), toRegion) + TAB;
                }
                if (param.equals("quantity")) {
                    listing += "1" + TAB;
                }
                if (param.equals("item-note")) {
                    if (item.getCondition() == 11) {
                        listing += config.getItemNoteNew() + TAB;
                    } else if (item.getCondition() == 2) {
                        if (item.getObiItem()) {
                            listing += config.getItemNoteObi() + TAB;
                        } else {
                            listing += config.getItemNoteUsed() + TAB;
                        }
                    }
                }
                if (param.equals("add-delete")) {
                    listing += "a" + TAB;
                }
                if (param.equals("will-ship-internationally")) {
                    String val = config.getWillShipInternationally();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + TAB;
                    }
                }
                if (param.equals("expedited-shipping")) {
                    String val = config.getExpeditedShipping();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + TAB;
                    }
                }
                if (param.equals("item-is-marketplace")) {
                    String val = config.getItemIsMarketplace();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + TAB;
                    }
                }
            }
            listing = listing.trim();
            listing += NEWLINE;
            byte[] listingBytes = listing.getBytes();
            dos.write(listingBytes);
            byteWritten += listingBytes.length;
        }
        dos.flush();
        if (byteWritten > 9 * 1024 * 1024) {
            byte[] md5 = dos.getMessageDigest().digest();
            dos.close();
            byteWritten = 0;
            dos = null;
            submitToAmazon(feedFile, md5);
        }
    }

    private void submitToAmazon(String feedFile, byte[] md5) {
        try {
            String md5Str = new String(Base64.encodeBase64(md5), "UTF-8");
            if (log.isDebugEnabled()) {
                log.debug("MD5 of the generated feed file: " + feedFile + " is " + md5Str);
            }
            new FeedSubmitter(feedFile, md5Str, toRegion).run();
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to submit feeds to amazon", e);
        }
    }

    private class FeedSubmitter implements Runnable {

        private String feedFile;
        private String md5;
        private String region;

        private String submissionId;

        public FeedSubmitter(final String feedFile, final String md5, final String region) {
            this.feedFile = feedFile;
            this.md5 = md5;
            this.region = region;
        }

        @Override
        public void run() {
            try {
                RepricerConfiguration config = new RepricerConfigurationDAO().getRepricer(region);
                AmazonAccessor amazonAccessor = new AmazonAccessor(region, config.getMarketplaceId(),
                        config.getSellerId());
                log.info("Submitting inventoryFeed " + feedFile + " to amazon.");
                String id = amazonAccessor.sendInventoryLoaderFeed(feedFile, md5);
                submissionIds.add(id);
                log.info("Submitted " + feedFile + " to amazon, submission id: " + id);
            } catch (Exception e) {
                log.error("Error processing the feed file", e);
            }
        }
    }
}
