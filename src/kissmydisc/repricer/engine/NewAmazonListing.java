package kissmydisc.repricer.engine;

import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.ListingConfiguration;

public class NewAmazonListing {

    private InventoryFeedItem item;

    private ListingConfiguration config;

    private static String HEADER = "product-id\tproduct-id-type\titem-condition\tprice\tsku\tquantity\tadd-delete\twill-ship-internationally\texpedited-shipping\titem-note\titem-is-marketplace\titem-name\titem-description\tcategory1\timage-url\tshipping-fee\tbrowse-path\tstorefront-feature\tboldface\tasin1\tasin2   asin3";

    private String[] requiredFields = { "sku", "product-id", "product-id-type", "item-condition", "price", "quantity",
            "add-delete", "will-ship-internationally", "item-is-marketplace" };

    public NewAmazonListing(InventoryFeedItem item, ListingConfiguration configuration) {
        this.item = item;
        this.config = configuration;
    }

    public String getListing() {
        String listing = "";

        return listing;
    }

    public static String getHeader() {
        return HEADER;
    }

}
