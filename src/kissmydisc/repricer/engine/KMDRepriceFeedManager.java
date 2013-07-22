package kissmydisc.repricer.engine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;

import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.FeedsDAO;
import kissmydisc.repricer.dao.KMDInventoryDAO;
import kissmydisc.repricer.model.PriceQuantityFeed;
import kissmydisc.repricer.utils.AppConfig;

public class KMDRepriceFeedManager implements RepriceFeedManager {

    private List<PriceQuantityFeed> feeds = new ArrayList<PriceQuantityFeed>();

    private static final int MAX_ITEMS = AppConfig.getInteger("MaxItemsPerKMDFeed", 1000);

    private String region;

    private long repriceId;

    public KMDRepriceFeedManager(final long repriceId) {
        this.repriceId = repriceId;
        this.region = "KMD";
    }

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(AmazonRepriceFeedManager.class);

    public synchronized void writeToFeedFile(PriceQuantityFeed feed) throws Exception {
        if (feed.getPrice() < 0 && feed.getQuantity() < 0) {
            // Some error in feed, drop it.
            return;
        }
        if (feeds.size() < MAX_ITEMS) {
            feeds.add(feed);
        }
        if (feeds.size() == MAX_ITEMS) {
            new KMDInventoryDAO().updateKMDPrice(feeds);
            log.info("Writing feeds to KMD Database");
            feeds = new ArrayList<PriceQuantityFeed>();
        }
    }

    public synchronized void flush() throws Exception {
        if (feeds.size() > 0) {
            new KMDInventoryDAO().updateKMDPrice(feeds);
        }
    }

    public void close() {
        // Do nothing here.
    }

}
