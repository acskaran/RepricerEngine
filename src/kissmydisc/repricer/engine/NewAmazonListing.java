package kissmydisc.repricer.engine;

import kissmydisc.repricer.feeds.AmazonFeed;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.InventoryLoaderConfiguration;

public class NewAmazonListing implements AmazonFeed {

    private InventoryFeedItem item;

    private InventoryLoaderConfiguration config;

    private static String HEADER = "product-id\tproduct-id-type\titem-condition\tprice\tsku\tquantity\tadd-delete\twill-ship-internationally\texpedited-shipping\titem-note\titem-is-marketplace\titem-name\titem-description\tcategory1\timage-url\tshipping-fee\tbrowse-path\tstorefront-feature\tboldface\tasin1\tasin2   asin3";

    private String[] requiredFields = { "sku", "product-id", "product-id-type", "item-condition", "price", "quantity",
            "add-delete", "will-ship-internationally", "item-is-marketplace" };

    public NewAmazonListing(InventoryFeedItem item, InventoryLoaderConfiguration configuration) {
        this.item = item;
        this.config = configuration;
    }

    public static String getHeader() {
        return HEADER;
    }

}
