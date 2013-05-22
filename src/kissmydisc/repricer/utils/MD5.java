package kissmydisc.repricer.utils;

import java.util.ArrayList;
import java.util.Map;

import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.ProductDetails;

public class MD5 {

    /*
    if (region.equals("JP")) {
        try {
            Map<String, Map<String, Pair<Float, Integer>>> productPriceAndQuantity = jpAccessor
                    .getProductDetails(skus);
            productDetails = new ArrayList<ProductDetails>();
            for (String sku : skus) {
                if (productPriceAndQuantity.containsKey(sku)) {
                    Map<String, Pair<Float, Integer>> pqWithCondition = productPriceAndQuantity.get(sku);
                    InventoryFeedItem item = items.get(sku);
                    Pair<Float, Integer> toReturn = null;
                    ProductDetails details = new ProductDetails();
                    details.setProductId(item.getProductId());
                    Pair<Float, Integer> pq = pqWithCondition.get("New");
                    if (pq != null) {
                        if (pq.getFirst() != null) {
                            details.setNewPrice(pq.getFirst());
                        }
                        if (pq.getSecond() != null) {
                            details.setNewQuantity(pq.getSecond());
                        }
                    }
                    if (item.getCondition() == 11) {
                        toReturn = pq;
                    }
                    pq = pqWithCondition.get("Used");
                    if (pq != null) {
                        if (pq.getFirst() != null) {
                            details.setUsedPrice(pq.getFirst());
                        }
                        if (pq.getSecond() != null) {
                            details.setUsedQuantity(pq.getSecond());
                        }
                    }
                    if (item.getCondition() >= 1 && item.getCondition() < 5) {
                        toReturn = pq;
                    }
                    if (toReturn != null) {
                        if (toReturn.getSecond() != null) {
                            quantityMap.put(sku, toReturn.getSecond());
                        }
                        if (toReturn.getFirst() != null) {
                            priceMap.put(sku, toReturn.getFirst());
                        }
                    } else {
                        log.warn("Matching product could not be found for " + item);
                    }
                    productDetails.add(details);
                }
            }
        } catch (Exception e) {
            log.error("Unable to get product price and quantity", e);
        }
    } else {
    */
}
