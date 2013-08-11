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
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.ListingConfiguration;
import kissmydisc.repricer.model.PriceQuantityFeed;
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

    private List<InventoryFeedItem> itemsBuffer;

    private DigestOutputStream dos = null;

    private int byteWritten = 0;

    private String directory = "feeds/";

    private String feedFile;

    private static final Log log = LogFactory.getLog(ListingCopier.class);

    public void copyListings() throws Exception {
        InventoryItemDAO inventoryDAO = new InventoryItemDAO();
        int limit = 100;
        String moreToken = null;
        Pair<Long, Long> latestInventory = new LatestInventoryDAO().getLatestInventoryWithCount(fromRegion);
        int totalAdded = 0;
        String identifiedMoreToken = null;
        String lastProcessed = null;
        Pair<Long, Long> inventoryDetails = new LatestInventoryDAO().getLatestInventoryWithCount(toRegion);
        long toInventoryId = inventoryDetails.getFirst();
        long totalItems = inventoryDetails.getSecond();
        long id = totalItems + 1;
        ListingConfiguration config = new ListingConfigurationDAO().getListingConfiguration(toRegion);
        try {
            do {
                Pair<List<InventoryFeedItem>, String> itemsAndMoreToken = inventoryDAO.getMatchingItems(
                        latestInventory.getFirst(), fromRegion, moreToken, limit);
                moreToken = itemsAndMoreToken.getSecond();
                List<InventoryFeedItem> items = itemsAndMoreToken.getFirst();
                List<InventoryFeedItem> newItems = new ArrayList<InventoryFeedItem>();
                for (InventoryFeedItem item : items) {
                    if (item.getQuantity() != 0) {
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
                        item.setSku(sku);
                        item.setRegion(toRegion);
                        item.setRegionProductId(toRegion + "_" + item.getProductId());
                        newItems.add(item);
                        if (lastProcessed != item.getRegionProductId()) {
                            identifiedMoreToken = lastProcessed;
                        }
                        lastProcessed = item.getRegionProductId();
                    }
                }
                totalAdded += newItems.size();
                new InventoryItemDAO().addItems(newItems);
                new LatestInventoryDAO().setLatestInventory(toRegion, toInventoryId, totalItems + totalAdded);
                buffer(toRegion, newItems, config);
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
        }
    }

    private void buffer(String toRegion, List<InventoryFeedItem> items, ListingConfiguration config) throws Exception {
        String TAB = "\t";
        String NEWLINE = "\n";
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
                    listing += item.getQuantity() + TAB;
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
                log.info("Submitted " + feedFile + " to amazon, submission id: " + id);
            } catch (Exception e) {
                log.error("Error processing the feed file", e);
            }
        }
    }
}