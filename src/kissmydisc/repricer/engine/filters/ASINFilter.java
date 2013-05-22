package kissmydisc.repricer.engine.filters;

import kissmydisc.repricer.model.InventoryFeedItem;

public class ASINFilter implements Filter {

    public ASINFilter(final InventoryFeedItem item) {

    }

    @Override
    public boolean shouldReprice() {
        return false;
    }

}
