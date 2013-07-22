package kissmydisc.repricer.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.AmazonAccessor;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.ProductDAO;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.ProductDetails;
import kissmydisc.repricer.utils.Pair;

public class ExternalDataManager {

    private Map<String, String> skuProductIdMap = new HashMap<String, String>();

    private Map<String, InventoryFeedItem> items = new HashMap<String, InventoryFeedItem>();

    private Map<String, Float> priceMap = new HashMap<String, Float>();

    private Map<String, Integer> quantityMap = new HashMap<String, Integer>();

    private Map<String, Float> weightMap = new HashMap<String, Float>();

    private List<String> skus = new ArrayList<String>();

    private List<String> productIds = new ArrayList<String>();

    private Long cachedWithin = null;

    private boolean cachedProductDetails = false;

    private static final Log log = LogFactory.getLog(ExternalDataManager.class);

    private AmazonAccessor jpAccessor = null;

    private AmazonAccessor currentRegionAccessor = null;

    private String region = null;

    private Map<String, Boolean> blackListMap = null;

    private Map<String, Float> lowestAmazonPriceMap = null;

    private Map<String, String> associations = null;

    private boolean cachedWeight = false;
    private boolean cachedPriceQuantity = false;

    private Map<String, AmazonAccessor> regionAccessorMap = new HashMap<String, AmazonAccessor>();

    private Map<String, Float> exchangeRates;

    /*
     * 1 = Used; Like New 2 = Used; Very Good 3 = Used; Good 4 = Used;
     * Acceptable 5 = Collectible; Like New 6 = Collectible; Very Good 7 =
     * Collectible; Good 8 = Collectible; Acceptable 11 = New
     */

    public ExternalDataManager(List<InventoryFeedItem> inventoryItems, Long cachedWithin, AmazonAccessor jpAccessor,
            AmazonAccessor regionAccessor) {
        for (InventoryFeedItem item : inventoryItems) {
            skuProductIdMap.put(item.getSku(), item.getProductId());
            skus.add(item.getSku());
            productIds.add(item.getProductId());
            region = item.getRegion();
            items.put(item.getSku(), item);
        }
        this.cachedWithin = cachedWithin;
        this.jpAccessor = jpAccessor;
        this.currentRegionAccessor = regionAccessor;
    }

    public ExternalDataManager(List<InventoryFeedItem> inventoryItems, Long cachedWithin, AmazonAccessor jpAccessor,
            Map<String, AmazonAccessor> regionAccessorMap, Map<String, Float> exchangeRates) {
        for (InventoryFeedItem item : inventoryItems) {
            skuProductIdMap.put(item.getSku(), item.getProductId());
            skus.add(item.getSku());
            productIds.add(item.getProductId());
            region = item.getRegion();
            items.put(item.getSku(), item);
        }
        this.cachedWithin = cachedWithin;
        this.jpAccessor = jpAccessor;
        this.regionAccessorMap = regionAccessorMap;
        this.exchangeRates = exchangeRates;
    }

    public Boolean isBlacklist(String sku) {
        // ASIN Filter
        if (blackListMap == null) {
            ProductDAO pdao = new ProductDAO();
            try {
                blackListMap = pdao.isBlackList(region, productIds);
                blackListMap.putAll(pdao.isBlackList(region, skus));
            } catch (DBException e) {
                log.error("Unable to get blacklist data from DB", e);
                blackListMap = Collections.emptyMap();
            }
        }
        if (blackListMap != null) {
            if (blackListMap.containsKey(skuProductIdMap.get(sku))) {
                return blackListMap.get(skuProductIdMap.get(sku));
            }
            if (blackListMap.containsKey(sku)) {
                return blackListMap.get(sku);
            }
            return false;
        }
        return false;
    }

    public Integer getQuantity(String sku) {
        if (quantityMap.containsKey(sku)) {
            return quantityMap.get(sku);
        } else if (cachedProductDetails) {
            return 0;
        } else {
            cache();
            return getQuantity(sku);
        }
    }

    private void cache() {
        final List<String> skuNotFoundInDB = new ArrayList<String>();
        try {
            try {
                ProductDAO dao = new ProductDAO();
                associations = dao.getAssociation(region, productIds);
                List<String> reqdProductIds = new ArrayList<String>();
                for (String productId : productIds) {
                    String id = getAssociatedProductId(productId);
                    reqdProductIds.add(id);
                }
                Map<String, ProductDetails> productDetails = dao.getProductDetails(reqdProductIds);
                skuNotFoundInDB.addAll(convertToRetrievableData(productDetails));
            } catch (DBException e) {
                log.error("Unable to retrieve product details from DB", e);
            }
            if (skuNotFoundInDB != null && skuNotFoundInDB.size() > 0) {
                final List<String> skuNotFoundInWeight = new ArrayList<String>();
                for (String sku : skuNotFoundInDB) {
                    if (weightMap.containsKey(sku) == false && !region.equals("KMD")) {
                        skuNotFoundInWeight.add(sku);
                    }
                }

                Thread getWeightThread = new Thread(new Runnable() {

                    public void run() {

                        // Get Weight From Amazon.
                        try {
                            Map<String, Float> weights = currentRegionAccessor.getWeight(skuNotFoundInDB);
                            Map<String, Float> productIdWeights = new HashMap<String, Float>();
                            if (weights != null) {
                                weightMap.putAll(weights);
                                for (String sku : weights.keySet()) {
                                    if (weights.containsKey(sku)) {
                                        String productId = skuProductIdMap.get(sku);
                                        productId = getAssociatedProductId(productId);
                                        productIdWeights.put(productId, weights.get(sku));
                                    }
                                }
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("Updating weights " + productIdWeights);
                            }
                            ProductDAO dao = new ProductDAO();
                            dao.setProductWeight(productIdWeights);
                        } catch (Exception e) {
                            log.error("Unable to Get Weight from Amazon/Update weight in DB", e);
                        } finally {
                            cachedWeight = true;
                        }
                    }
                });
                if (skuNotFoundInWeight.size() > 0) {
                    getWeightThread.start();
                } else {
                    cachedWeight = true;
                }

                Thread getPQThread = new Thread(new Runnable() {

                    public void run() {

                        // Retrieve price and quantity from Amazon
                        try {
                            cachePriceAndQuantity(skuNotFoundInDB);
                        } finally {
                            cachedPriceQuantity = true;
                        }
                    }
                });
                getPQThread.start();
                while (!cachedPriceQuantity || !cachedWeight) {
                    log.info("Waiting for caching price/quantity and weight PQ:" + cachedPriceQuantity + ", W:"
                            + cachedWeight);
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                // Retrieve weight from Amazon if we do not have the data
                // locally.

            }
        } finally {
            cachedProductDetails = true;
        }
    }

    public Float getLowestRegionalPrice(String sku) {
        if (lowestAmazonPriceMap == null) {
            Map<String, Map<String, Pair<Float, Integer>>> productPriceAndQuantity = new HashMap<String, Map<String, Pair<Float, Integer>>>();
            lowestAmazonPriceMap = new HashMap<String, Float>();

            if ("KMD".equals(region)) {
                String[] order = { "US", "UK", "DE", "IT", "ES", "CA" };
                List<String> leftOutProductIds = productIds;
                for (String entry : order) {
                    if (log.isDebugEnabled()) {
                        log.debug("Getting competitor price from " + entry + " for " + leftOutProductIds + " "
                                + lowestAmazonPriceMap);
                    }
                    AmazonAccessor currentAccessor = regionAccessorMap.get(entry);
                    Float exchangeRate = 1.0F;
                    String currency = "USD";
                    if (entry.equals("UK"))
                        currency = "GBP";
                    if (entry.equals("FR") || entry.equals("DE") || entry.equals("IT") || entry.equals("ES"))
                        currency = "EUR";
                    if (entry.equals("CA"))
                        currency = "CAD";
                    if (exchangeRates.containsKey(currency)) {
                        exchangeRate = exchangeRates.get(currency);
                    }
                    try {
                        productPriceAndQuantity = currentAccessor.getProductDetailsByASIN(leftOutProductIds, true);
                        for (Map.Entry<String, Map<String, Pair<Float, Integer>>> productEntry : productPriceAndQuantity
                                .entrySet()) {
                            for (Map.Entry<String, Pair<Float, Integer>> priceEntry : productEntry.getValue()
                                    .entrySet()) {
                                if (priceEntry.getValue() != null) {
                                    Float price = priceEntry.getValue().getFirst();
                                    if (price > 0) {
                                        price = price * exchangeRate;
                                        priceEntry.getValue().setFirst(price);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Unable to retrieve lowest prices from Amazon - " + entry, e);
                    }
                    leftOutProductIds = cacheLowestRegionalPrices(productPriceAndQuantity, lowestAmazonPriceMap);
                    if (leftOutProductIds == null || leftOutProductIds.isEmpty()) {
                        break;
                    }
                }

            } else {
                try {
                    productPriceAndQuantity = currentRegionAccessor.getProductDetailsByASIN(productIds, true);
                    cacheLowestRegionalPrices(productPriceAndQuantity, lowestAmazonPriceMap);
                } catch (Exception e) {
                    log.error("Unable to retrieve lowest prices from Amazon.", e);
                }
            }

        }
        if (lowestAmazonPriceMap.containsKey(sku)) {
            return lowestAmazonPriceMap.get(sku);
        } else {
            return -1F;
        }
    }

    private List<String> cacheLowestRegionalPrices(
            Map<String, Map<String, Pair<Float, Integer>>> productPriceAndQuantity,
            Map<String, Float> lowestAmazonPriceMap) {
        Map<Long, Float> toUpdate = new HashMap<Long, Float>();
        List<String> priceNotAvailable = new ArrayList<String>();
        for (String tempSku : skus) {
            if (!lowestAmazonPriceMap.containsKey(tempSku)) {
                String productId = items.get(tempSku).getProductId();
                InventoryFeedItem item = items.get(tempSku);
                if (productPriceAndQuantity.containsKey(productId)) {
                    Map<String, Pair<Float, Integer>> pqWithCondition = productPriceAndQuantity.get(productId);
                    Pair<Float, Integer> toReturn = null;
                    Pair<Float, Integer> pq = pqWithCondition.get("New");
                    if (item.getCondition() == 11) {
                        toReturn = pq;
                    }
                    pq = pqWithCondition.get("Used");
                    if (item.getCondition() >= 1 && item.getCondition() <= 10) {
                        toReturn = pq;
                    }
                    if (toReturn != null) {
                        if (toReturn.getFirst() != null && toReturn.getFirst() > 0) {
                            lowestAmazonPriceMap.put(tempSku, toReturn.getFirst());
                            toUpdate.put(item.getInventoryItemId(), toReturn.getFirst());
                        } else {
                            priceNotAvailable.add(productId);
                        }
                    } else {
                        priceNotAvailable.add(productId);
                    }
                } else {
                    priceNotAvailable.add(productId);
                }
            }
        }
        if (toUpdate != null || toUpdate.size() > 0) {
            try {
                new InventoryItemDAO().updateLowestAmazonPrice(toUpdate);
            } catch (DBException e) {
                log.error("Unable to update lowest prices in DB: " + toUpdate, e);
            }
        }
        return priceNotAvailable;
    }

    private String getAssociatedProductId(String productId) {
        if (associations != null && associations.containsKey(productId)) {
            productId = associations.get(productId);
        }
        return productId;
    }

    private void cachePriceAndQuantity(List<String> skus) {
        // Retrieve product details from Amazon JP.
        List<ProductDetails> productDetails = null;
        try {
            List<String> reqdProductIds = new ArrayList<String>();
            for (String productId : productIds) {
                String id = getAssociatedProductId(productId);
                reqdProductIds.add(id);
            }
            Map<String, Map<String, Pair<Float, Integer>>> productPriceAndQuantity = jpAccessor
                    .getProductDetailsByASIN(reqdProductIds, false);
            productDetails = new ArrayList<ProductDetails>();
            for (String sku : skus) {
                String productId = items.get(sku).getProductId();
                productId = getAssociatedProductId(productId);
                InventoryFeedItem item = items.get(sku);
                if (productPriceAndQuantity.containsKey(productId)) {
                    Map<String, Pair<Float, Integer>> pqWithCondition = productPriceAndQuantity.get(productId);
                    Pair<Float, Integer> toReturn = null;
                    ProductDetails details = new ProductDetails();
                    details.setProductId(productId);
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
                    if (item.getCondition() >= 1 && item.getCondition() <= 10) {
                        toReturn = pq;
                    }
                    if (toReturn != null) {
                        if (toReturn.getSecond() != null) {
                            if (toReturn.getSecond() >= 0) {
                                quantityMap.put(sku, toReturn.getSecond());
                            } else {
                                quantityMap.put(sku, 0);
                            }
                        }
                        if (toReturn.getFirst() != null) {
                            priceMap.put(sku, toReturn.getFirst());
                        }
                    } else {
                        quantityMap.put(sku, 0);
                        log.warn("Matching product could not be found for " + item);
                    }
                    productDetails.add(details);
                }
            }
        } catch (Exception e) {
            log.error("Unable to get product price and quantity", e);
        }

        if (productDetails != null && productDetails.size() > 0) {
            // Update in DB.
            try {
                ProductDAO dao = new ProductDAO();
                dao.updateProductPriceQuantityData(productDetails);
            } catch (DBException e) {
                log.error("Unable to update product details in DB", e);
            }
        }
    }

    private List<String> convertToRetrievableData(Map<String, ProductDetails> productDetails) {
        List<String> skuNotFoundInDB = new ArrayList<String>();
        if (productDetails != null) {
            for (Map.Entry<String, InventoryFeedItem> entryItem : items.entrySet()) {
                InventoryFeedItem item = entryItem.getValue();
                String sku = entryItem.getKey();
                String productId = item.getProductId();
                productId = getAssociatedProductId(productId);
                if (productDetails.containsKey(productId)) {
                    ProductDetails details = productDetails.get(productId);
                    if (details.getWeight() != null) {
                        weightMap.put(sku, details.getWeight());
                    }
                    if (item.getCondition() == 11) {
                        priceMap.put(sku, details.getNewPrice());
                        if (details.getNewQuantity() == -1) {
                            quantityMap.put(sku, 0);
                        } else {
                            quantityMap.put(sku, details.getNewQuantity());
                        }
                    } else {
                        priceMap.put(sku, details.getUsedPrice());
                        if (details.getUsedQuantity() == -1) {
                            quantityMap.put(sku, 0);
                        } else {
                            quantityMap.put(sku, details.getUsedQuantity());
                        }
                    }
                    if (cachedWithin >= 0
                            && details.getLastUpdated().before(new Date(System.currentTimeMillis() - cachedWithin))) {
                        skuNotFoundInDB.add(sku);
                    }
                } else {
                    skuNotFoundInDB.add(sku);
                }
            }
        }
        return skuNotFoundInDB;
    }

    public Float getLowestAmazonPrice(String sku) {
        if (priceMap.containsKey(sku)) {
            return priceMap.get(sku);
        } else if (cachedProductDetails) {
            return -1F;
        } else {
            cache();
            return getLowestAmazonPrice(sku);
        }
    }

    public Float getWeight(String sku) {
        if (weightMap.containsKey(sku)) {
            return weightMap.get(sku);
        } else if (cachedProductDetails) {
            return -1F;
        } else {
            cache();
            return getWeight(sku);
        }
    }

}
