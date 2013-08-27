package kissmydisc.repricer.engine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.feeds.AmazonFeed;
import kissmydisc.repricer.feeds.InventoryLoaderFeed;
import kissmydisc.repricer.model.AmazonSubmission;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.InventoryLoaderConfiguration;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.model.AmazonSubmission.AmazonSubmissionType;
import kissmydisc.repricer.utils.Constants;
import kissmydisc.repricer.utils.PriceUtils;

public class AmazonInventoryLoaderFeedManager implements RepriceFeedManager {

    private DigestOutputStream dos;

    private String feedFile;

    private String directory = "feeds/";

    private byte[] md5;

    private int byteWritten = 0;

    private static final Log log = LogFactory.getLog(AmazonInventoryLoaderFeedManager.class);

    private String region;

    private long sourceId;

    private List<AmazonSubmission> submissions = new ArrayList<AmazonSubmission>();

    private InventoryLoaderConfiguration config;

    public AmazonInventoryLoaderFeedManager(String region, long sourceId, InventoryLoaderConfiguration config) {
        this.region = region;
        this.sourceId = sourceId;
        this.config = config;
    }

    @Override
    public void writeToFeedFile(AmazonFeed amzFeed) throws Exception {
        if (amzFeed instanceof InventoryLoaderFeed) {
            InventoryLoaderFeed feed = (InventoryLoaderFeed) amzFeed;
            buffer(region, feed.getItems());
        }
    }

    private void buffer(String region, List<InventoryFeedItem> items) throws Exception {
        if (dos == null) {
            feedFile = directory + "InventoryLoader-" + region + "-" + System.currentTimeMillis();
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
                    listing += item.getSku() + Constants.TAB;
                }
                if (param.equals("product-id")) {
                    listing += item.getProductId() + Constants.TAB;
                }
                if (param.equals("product-id-type")) {
                    listing += "1" + Constants.TAB;
                }
                if (param.equals("item-condition")) {
                    listing += item.getCondition() + Constants.TAB;
                }
                if (param.equals("price")) {
                    listing += PriceUtils.getPrice(item.getPrice(), region) + Constants.TAB;
                }
                if (param.equals("quantity")) {
                    listing += config.getQuantity() + Constants.TAB;
                }
                if (param.equals("item-note")) {
                    if (item.getCondition() == 11) {
                        listing += config.getItemNoteNew() + Constants.TAB;
                    } else if (item.getCondition() == 2) {
                        if (item.getObiItem()) {
                            listing += config.getItemNoteObi() + Constants.TAB;
                        } else {
                            listing += config.getItemNoteUsed() + Constants.TAB;
                        }
                    }
                }
                if (param.equals("add-delete")) {
                    listing += config.getAddDelete() + Constants.TAB;
                }
                if (param.equals("will-ship-internationally")) {
                    String val = config.getWillShipInternationally();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + Constants.TAB;
                    }
                }
                if (param.equals("expedited-shipping")) {
                    String val = config.getExpeditedShipping();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + Constants.TAB;
                    }
                }
                if (param.equals("item-is-marketplace")) {
                    String val = config.getItemIsMarketplace();
                    if (val != null && val.trim() != "") {
                        listing += val.trim() + Constants.TAB;
                    }
                }
            }
            listing = listing.trim();
            listing += Constants.NEWLINE;
            byte[] listingBytes = listing.getBytes();
            dos.write(listingBytes);
            byteWritten += listingBytes.length;
        }
        dos.flush();
        if (byteWritten > 9 * 1024 * 1024) {
            flush();
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
                AmazonSubmission submission = new AmazonSubmission();
                submission.setFilePath(feedFile);
                submission.setMd5(md5);
                submission.setSourceId(sourceId);
                submission.setSubmissionId(id);
                submission.setType(AmazonSubmissionType.INV_LOADER);
                submissions.add(submission);
                log.info("Submitted " + feedFile + " to amazon, submission id: " + id);
            } catch (Exception e) {
                log.error("Error processing the feed file", e);
            }
        }
    }

    @Override
    public void flush() throws Exception {
        if (feedFile != null && dos != null) {
            md5 = dos.getMessageDigest().digest();
            dos.close();
            byteWritten = 0;
            dos = null;
            try {
                String md5Str = new String(Base64.encodeBase64(md5), "UTF-8");
                if (log.isDebugEnabled()) {
                    log.debug("MD5 of the generated feed file: " + feedFile + " is " + md5Str);
                }
                new FeedSubmitter(feedFile, md5Str, region).run();
            } catch (UnsupportedEncodingException e) {
                log.error("Unable to submit feeds to amazon", e);
            }
        }
    }

    @Override
    public List<AmazonSubmission> getSubmissions() {
        return submissions;
    }

}
