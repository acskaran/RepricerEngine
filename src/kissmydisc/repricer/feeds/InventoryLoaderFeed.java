package kissmydisc.repricer.feeds;

import java.util.List;

import kissmydisc.repricer.model.InventoryFeedItem;

public class InventoryLoaderFeed implements AmazonFeed {

    private List<InventoryFeedItem> items;

    public InventoryLoaderFeed(List<InventoryFeedItem> items) {
        this.setItems(items);
    }

    public List<InventoryFeedItem> getItems() {
        return items;
    }

    public void setItems(List<InventoryFeedItem> items) {
        this.items = items;
    }

}
