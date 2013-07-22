package kissmydisc.repricer.dao;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kissmydisc.repricer.utils.AppConfig;
import kissmydisc.repricer.utils.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.amazonaws.mws.MarketplaceWebService;
import com.amazonaws.mws.MarketplaceWebServiceClient;
import com.amazonaws.mws.MarketplaceWebServiceConfig;
import com.amazonaws.mws.model.ContentType;
import com.amazonaws.mws.model.FeedSubmissionInfo;
import com.amazonaws.mws.model.IdList;
import com.amazonaws.mws.model.SubmitFeedRequest;
import com.amazonaws.mws.model.SubmitFeedResponse;
import com.amazonaws.mws.model.SubmitFeedResult;
import com.amazonservices.mws.products.MarketplaceWebServiceProducts;
import com.amazonservices.mws.products.MarketplaceWebServiceProductsClient;
import com.amazonservices.mws.products.MarketplaceWebServiceProductsConfig;
import com.amazonservices.mws.products.MarketplaceWebServiceProductsException;
import com.amazonservices.mws.products.model.ASINListType;
import com.amazonservices.mws.products.model.AttributeSetList;
import com.amazonservices.mws.products.model.CompetitivePriceList;
import com.amazonservices.mws.products.model.CompetitivePriceType;
import com.amazonservices.mws.products.model.CompetitivePricingType;
import com.amazonservices.mws.products.model.GetCompetitivePricingForASINRequest;
import com.amazonservices.mws.products.model.GetCompetitivePricingForASINResponse;
import com.amazonservices.mws.products.model.GetCompetitivePricingForASINResult;
import com.amazonservices.mws.products.model.GetCompetitivePricingForSKURequest;
import com.amazonservices.mws.products.model.GetCompetitivePricingForSKUResponse;
import com.amazonservices.mws.products.model.GetCompetitivePricingForSKUResult;
import com.amazonservices.mws.products.model.GetLowestOfferListingsForASINRequest;
import com.amazonservices.mws.products.model.GetLowestOfferListingsForASINResponse;
import com.amazonservices.mws.products.model.GetLowestOfferListingsForASINResult;
import com.amazonservices.mws.products.model.GetMatchingProductForIdRequest;
import com.amazonservices.mws.products.model.GetMatchingProductForIdResponse;
import com.amazonservices.mws.products.model.GetMatchingProductForIdResult;
import com.amazonservices.mws.products.model.IdListType;
import com.amazonservices.mws.products.model.LowestOfferListingList;
import com.amazonservices.mws.products.model.LowestOfferListingType;
import com.amazonservices.mws.products.model.MoneyType;
import com.amazonservices.mws.products.model.NumberOfOfferListingsList;
import com.amazonservices.mws.products.model.OfferListingCountType;
import com.amazonservices.mws.products.model.PriceType;
import com.amazonservices.mws.products.model.Product;
import com.amazonservices.mws.products.model.ProductList;
import com.amazonservices.mws.products.model.QualifiersType;
import com.amazonservices.mws.products.model.SellerSKUListType;

public class AmazonAccessor {

    private static Map<String, MarketplaceWebServiceProducts> PRODUCT_SERVICE_MAP = new Hashtable<String, MarketplaceWebServiceProducts>();

    private static Map<String, MarketplaceWebService> FEEDS_SERVICE_MAP = new Hashtable<String, MarketplaceWebService>();

    private MarketplaceWebServiceProducts productService;

    private static final int MAX_TRIES = 20;

    private static final int MAX_RETRY = AppConfig.getInteger("ThrottledRequest.MaxRetry", 5);

    private MarketplaceWebService feedsService;

    private String marketplaceId;

    private String sellerId;

    private String region;

    private static boolean initialized = false;

    private static final Map<String, AmazonAccessMonitor> GET_WEIGHT_MONITOR = new Hashtable<String, AmazonAccessMonitor>();

    private static final Map<String, AmazonAccessMonitor> GET_LOWEST_PRICE_MONITOR = new Hashtable<String, AmazonAccessMonitor>();

    private static final Map<String, AmazonAccessMonitor> GET_LOWEST_OFFER_MONITOR = new Hashtable<String, AmazonAccessMonitor>();

    private static final Log log = LogFactory.getLog(AmazonAccessor.class);

    private static final int GET_WEIGHT_RESTORE_RATE = AppConfig.getInteger("GetWeight.RestoreRate", 2);

    private static final int GET_PRICE_RESTORE_RATE = AppConfig.getInteger("GetPrice.RestoreRate", 8);

    private static MarketplaceWebService getFeedsServiceClient(String region, Properties properties) {
        String accessKey = properties.getProperty(region + "_ACCESS_KEY");
        String secretKey = properties.getProperty(region + "_SECRET_KEY");
        MarketplaceWebService service = new MarketplaceWebServiceClient(accessKey, secretKey, "SimplyReliableRepricer",
                "1.0", getClientConfig(region));
        return service;
    }

    private static MarketplaceWebServiceConfig getClientConfig(String region) {
        MarketplaceWebServiceConfig config = new MarketplaceWebServiceConfig();
        if (region.equals("US")) {
            config.setServiceURL("https://mws.amazonservices.com");
        } else if (region.equals("JP")) {
            config.setServiceURL("https://mws.amazonservices.jp");
        } else if (region.equals("CA")) {
            config.setServiceURL("https://mws.amazonservices.ca");
        } else if (region.equals("UK") || region.equals("FR") || region.equals("DE") || region.equals("IT")
                || region.equals("ES")) {
            config.setServiceURL("https://mws-eu.amazonservices.com");
        } else if (region.equals("CN")) {
            config.setServiceURL("https://mws.amazonservices.com.cn");
        }
        return config;
    }

    private static MarketplaceWebServiceProducts getProductServiceClient(String region, Properties properties,
            boolean async) {
        String accessKey = properties.getProperty(region + "_ACCESS_KEY");
        String secretKey = properties.getProperty(region + "_SECRET_KEY");
        if (async == false) {
            return new MarketplaceWebServiceProductsClient(accessKey, secretKey, "SimplyReliableRepricer", "1.0",
                    getConfig(region));
        } else {
            return null;
        }

    }

    private static MarketplaceWebServiceProductsConfig getConfig(String region) {
        MarketplaceWebServiceProductsConfig config = new MarketplaceWebServiceProductsConfig();
        if (region.equals("US")) {
            config.setServiceURL("https://mws.amazonservices.com/Products/2011-10-01");
        } else if (region.equals("JP")) {
            config.setServiceURL("https://mws.amazonservices.jp/Products/2011-10-01");
        } else if (region.equals("CA")) {
            config.setServiceURL("https://mws.amazonservices.ca/Products/2011-10-01");
        } else if (region.equals("UK") || region.equals("FR") || region.equals("DE") || region.equals("IT")
                || region.equals("ES")) {
            config.setServiceURL("https://mws-eu.amazonservices.com/Products/2011-10-01");
        } else if (region.equals("CN")) {
            config.setServiceURL("https://mws.amazonservices.com.cn/Products/2011-10-01");
        }
        config.setMaxConnections(10);
        config.setMaxErrorRetry(3);
        return config;
    }

    public static ContentType getContentType(String region) {
        if ("JP".equals(region)) {
            return ContentType.TextTabSeparatedJP;
        } else if ("CN".equals(region)) {
            return ContentType.TextTabSeparatedCN;
        } else {
            return ContentType.TextTabSeparatedRest;
        }
    }

    public AmazonAccessor(String region, String marketplaceId, String sellerId) {
        productService = PRODUCT_SERVICE_MAP.get(region);
        feedsService = FEEDS_SERVICE_MAP.get(region);
        this.marketplaceId = marketplaceId;
        this.sellerId = sellerId;
        this.region = region;
    }

    private Map<String, Float> getLowestPrice(Map<String, String> skuCondition) throws Exception {
        if (AppConfig.getBoolean("ShouldNotGetLowestPriceFromAmazon", false)) {
            Map<String, Float> testMap = new HashMap<String, Float>();
            for (String key : skuCondition.keySet()) {
                testMap.put(key, 200F);
            }
            return testMap;
        }
        int tries = 0;
        int waitTime = 0;
        AmazonAccessMonitor getLowestPriceMonitor = GET_LOWEST_PRICE_MONITOR.get(region);
        while (tries++ < 10) {
            waitTime = getLowestPriceMonitor.addRequest(skuCondition.size());
            if (waitTime == 0) {
                break;
            } else {
                Thread.sleep(waitTime);
                log.debug("Waiting to get request quota.");
            }
        }
        if (waitTime > 0) {
            log.warn("A GetLowestPrice Request is starving for quota - executing without complying with policy.");
        }
        GetCompetitivePricingForSKURequest request = new GetCompetitivePricingForSKURequest();
        request.setMarketplaceId(marketplaceId);
        request.setSellerId(sellerId);
        List<String> skus = new ArrayList<String>();
        for (String s : skuCondition.keySet()) {
            skus.add(s);
        }
        SellerSKUListType list = new SellerSKUListType(skus);
        request.setSellerSKUList(list);
        GetCompetitivePricingForSKUResponse response = productService.getCompetitivePricingForSKU(request);
        Map<String, Float> lowestPriceMap = new HashMap<String, Float>();
        for (GetCompetitivePricingForSKUResult result : response.getGetCompetitivePricingForSKUResult()) {
            if (result.isSetStatus()) {
                if ("Success".equalsIgnoreCase(result.getStatus())) {
                    String sku = result.getSellerSKU();
                    Product product = result.getProduct();
                    CompetitivePricingType competitivePricing = product.getCompetitivePricing();
                    CompetitivePriceList compList = competitivePricing.getCompetitivePrices();
                    for (CompetitivePriceType compPrice : compList.getCompetitivePrice()) {
                        if (skuCondition.get(sku).equalsIgnoreCase(compPrice.getCondition()) && compPrice.isSetPrice()) {
                            PriceType priceType = compPrice.getPrice();
                            MoneyType moneyType = priceType.getListingPrice();
                            BigDecimal amount = moneyType.getAmount();
                            lowestPriceMap.put(sku, amount.floatValue());
                        }
                    }
                }
            }
        }
        return lowestPriceMap;
    }

    private Map<String, Map<String, Pair<Float, Integer>>> getProductDetails(List<String> skus) throws Exception {
        if (AppConfig.getBoolean("ShouldNotGetLowestPriceFromAmazon", false)) {
            Map<String, Map<String, Pair<Float, Integer>>> testMap = new HashMap<String, Map<String, Pair<Float, Integer>>>();
            for (String key : skus) {
                Map<String, Pair<Float, Integer>> mp = new HashMap<String, Pair<Float, Integer>>();
                mp.put("Used", new Pair<Float, Integer>(200F, 10));
                mp.put("New", new Pair<Float, Integer>(250F, 15));
                testMap.put(key, mp);
            }
            return testMap;
        }
        int tries = 0;
        int waitTime = 0;
        AmazonAccessMonitor getLowestPriceMonitor = GET_LOWEST_PRICE_MONITOR.get(region);
        while (tries++ < 10) {
            waitTime = getLowestPriceMonitor.addRequest(skus.size());
            if (waitTime == 0) {
                break;
            } else {
                Thread.sleep(waitTime);
                log.debug("Waiting to get request quota.");
            }
        }
        if (waitTime > 0) {
            log.warn("A GetProductDetails Request is starving for quota - executing without complying with policy.");
        }
        GetCompetitivePricingForSKURequest request = new GetCompetitivePricingForSKURequest();
        request.setMarketplaceId(marketplaceId);
        request.setSellerId(sellerId);
        SellerSKUListType list = new SellerSKUListType(skus);
        request.setSellerSKUList(list);
        GetCompetitivePricingForSKUResponse response = productService.getCompetitivePricingForSKU(request);
        Map<String, Map<String, Pair<Float, Integer>>> lowestPriceMap = new HashMap<String, Map<String, Pair<Float, Integer>>>();
        for (GetCompetitivePricingForSKUResult result : response.getGetCompetitivePricingForSKUResult()) {
            if (result.isSetStatus()) {
                if ("success".equalsIgnoreCase(result.getStatus())) {
                    String sku = result.getSellerSKU();
                    Product product = result.getProduct();
                    CompetitivePricingType competitivePricing = product.getCompetitivePricing();
                    CompetitivePriceList compList = competitivePricing.getCompetitivePrices();
                    Float price = null;
                    Map<String, Pair<Float, Integer>> mp = new HashMap<String, Pair<Float, Integer>>();
                    for (CompetitivePriceType compPrice : compList.getCompetitivePrice()) {
                        if (compPrice.isSetPrice()) {
                            PriceType priceType = compPrice.getPrice();
                            MoneyType moneyType = priceType.getListingPrice();
                            BigDecimal amount = moneyType.getAmount();
                            price = amount.floatValue();
                            mp.put(compPrice.getCondition(), new Pair<Float, Integer>(price, -1));
                        }
                    }
                    NumberOfOfferListingsList offerListings = competitivePricing.getNumberOfOfferListings();
                    offerListings.getOfferListingCount();
                    Integer quantity = null;
                    for (OfferListingCountType offerListing : offerListings.getOfferListingCount()) {
                        String condition = offerListing.getCondition();
                        quantity = offerListing.getValue();
                        if (mp.containsKey(condition)) {
                            mp.get(condition).setSecond(quantity);
                        } else {
                            mp.put(condition, new Pair<Float, Integer>(-1F, quantity));
                        }
                    }
                    lowestPriceMap.put(sku, mp);
                }
            }
        }
        return lowestPriceMap;
    }

    public String sendFeed(String feedFile, String md5) throws Exception {
        if (AppConfig.getBoolean("ShouldNotSubmitFeedToAmazon", false)) {
            return UUID.randomUUID().toString();
        }
        SubmitFeedRequest request = new SubmitFeedRequest();
        request.setMerchant(sellerId);
        List<String> mkt = new ArrayList<String>();
        mkt.add(marketplaceId);
        IdList list = new IdList(mkt);
        request.setMarketplaceIdList(list);
        request.setFeedType("_POST_FLAT_FILE_PRICEANDQUANTITYONLY_UPDATE_DATA_");
        request.setContentMD5(md5);
        request.setFeedContent(new FileInputStream(feedFile));
        request.setContentType(getContentType(region));
        SubmitFeedResponse response = feedsService.submitFeed(request);
        if (response.isSetSubmitFeedResult()) {
            SubmitFeedResult result = response.getSubmitFeedResult();
            if (result.isSetFeedSubmissionInfo()) {
                FeedSubmissionInfo info = result.getFeedSubmissionInfo();
                return info.getFeedSubmissionId();
            }
        }
        return null;
    }

    public Map<String, Float> getWeight(List<String> reqId) throws Exception {
        if (AppConfig.getBoolean("ShouldNotGetWeightFromAmazon", false)) {
            Map<String, Float> testMap = new HashMap<String, Float>();
            return testMap;
        }
        int maxBatchSize = 5;
        int reqsReqd = reqId.size() / maxBatchSize;

        Map<String, Float> weightMap = new HashMap<String, Float>();

        AmazonAccessMonitor getWeightMonitor = GET_WEIGHT_MONITOR.get(region);

        for (int req = 0; req <= reqsReqd; req++) {

            List<String> ids = reqId.subList(req * maxBatchSize, (Math.min((req + 1) * maxBatchSize, reqId.size())));

            if (ids.size() == 0) {
                continue;
            }

            boolean shouldRetry = true;
            for (int retry = 0; shouldRetry && retry < MAX_RETRY; retry++) {
                shouldRetry = false;
                int tries = 0;
                int waitTime = 0;
                while (tries++ < 2 * MAX_TRIES) {
                    waitTime = getWeightMonitor.addRequest(ids.size());
                    if (waitTime == 0) {
                        break;
                    } else {
                        Thread.sleep(waitTime);
                        log.debug("Waiting to get request quota.");
                    }
                }
                if (waitTime > 0) {
                    log.warn("A GetWeight Request is starving for quota - executing without complying with policy.");
                }

                GetMatchingProductForIdRequest gmpfid = new GetMatchingProductForIdRequest();
                gmpfid.setIdType("SellerSKU");

                IdListType idList = new IdListType();
                idList.setId(ids);
                gmpfid.setIdList(idList);

                gmpfid.setMarketplaceId(marketplaceId);
                gmpfid.setSellerId(sellerId);

                try {
                    long startTime = System.currentTimeMillis();
                    GetMatchingProductForIdResponse resp = productService.getMatchingProductForId(gmpfid);
                    List<GetMatchingProductForIdResult> lst = resp.getGetMatchingProductForIdResult();
                    for (GetMatchingProductForIdResult r : lst) {
                        String sku = r.getId();
                        if (r.isSetProducts()) {
                            ProductList p1 = r.getProducts();
                            for (Product p : p1.getProduct()) {
                                AttributeSetList l = p.getAttributeSets();
                                for (Object o : l.getAny()) {
                                    Node n = (Node) o;
                                    NodeList m = n.getChildNodes();
                                    for (int i = 0; i < m.getLength(); i++) {
                                        Node nn = m.item(i);
                                        if ("ns2:PackageDimensions".equals(nn.getNodeName())) {
                                            NodeList nnl = nn.getChildNodes();
                                            for (int j = 0; j < nnl.getLength(); j++) {
                                                Node nnn = nnl.item(j);
                                                if ("ns2:Weight".equals(nnn.getNodeName())) {
                                                    Float weight = 0.0F;
                                                    if (nnn.getTextContent() != null) {
                                                        weight = Float.parseFloat(nnn.getTextContent());
                                                    }
                                                    String units = nnn.getAttributes().getNamedItem("Units")
                                                            .getTextContent();
                                                    units = units.toLowerCase();
                                                    if ("pounds".equalsIgnoreCase(units)) {
                                                        weight = weight * 453.592F; // (Converting
                                                                                    // to
                                                                                    // grams
                                                                                    // from
                                                                                    // pounds);
                                                    }
                                                    weightMap.put(sku, weight);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Time taken for GetWeight call: " + (System.currentTimeMillis() - startTime)
                                + ", Region=" + region);
                    }
                } catch (Exception e) {
                    if (e.getMessage().contains("throttled") || e.getMessage().contains("Throttled")) {
                        shouldRetry = true;
                        if (log.isDebugEnabled()) {
                            log.debug("Retrying throttled GetWeight request for " + region);
                        }
                    }
                    log.error("Error getting weight for items, returning the weight got so far: " + weightMap, e);
                }
            }
        }
        return weightMap;
    }

    public Map<String, Map<String, Pair<Float, Integer>>> getProductDetailsByASIN(List<String> productIds,
            boolean onlyPrice) throws Exception {
        if (AppConfig.getBoolean("ShouldNotGetLowestPriceFromAmazon", false)) {
            Map<String, Map<String, Pair<Float, Integer>>> testMap = new HashMap<String, Map<String, Pair<Float, Integer>>>();
            for (String key : productIds) {
                Map<String, Pair<Float, Integer>> mp = new HashMap<String, Pair<Float, Integer>>();
                mp.put("Used", new Pair<Float, Integer>(200F, 10));
                mp.put("New", new Pair<Float, Integer>(250F, 15));
                testMap.put(key, mp);
            }
            return testMap;
        }

        final List<String> products = new ArrayList<String>();
        for (String productId : productIds) {
            if (!products.contains(productId)) {
                products.add(productId);
            }
        }

        GetLowestOfferingsThread getLowestOfferings = new GetLowestOfferingsThread(products, productService);
        Thread getLowestOfferingThread = new Thread(getLowestOfferings);
        getLowestOfferingThread.start();

        GetCompetitivePricingForASINResponse response = null;

        if (!onlyPrice) {
            GetCompetitivePriceThread getCompetitivePrice = new GetCompetitivePriceThread(products, productService);
            Thread getCompetitivePriceThread = new Thread(getCompetitivePrice);
            getCompetitivePriceThread.start();
            while (!getCompetitivePrice.isDone()) {
                try {
                    Thread.sleep(50);
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for getCompetitivePrice call to Complete for " + products);
                    }
                } catch (Exception e) {
                }
            }
            response = getCompetitivePrice.getResponse();
        }
        while (!getLowestOfferings.isDone()) {
            try {
                Thread.sleep(50);
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for getLowestOfferings call to Complete for " + products);
                }
            } catch (Exception e) {
            }
        }

        GetLowestOfferListingsForASINResponse getLowestResponse = getLowestOfferings.getResponse();

        Map<String, Map<String, Pair<Float, Integer>>> lowestPriceMap = new HashMap<String, Map<String, Pair<Float, Integer>>>();

        if (getLowestResponse != null) {

            Map<String, List<String>> leftOutAsins = parseGetLowestOfferListingsResponse(getLowestResponse,
                    lowestPriceMap);

            if (leftOutAsins != null && leftOutAsins.size() > 0) {
                GetLowestOfferingsThread getLowestOfferingsCall = new GetLowestOfferingsThread(leftOutAsins,
                        productService);
                log.info("Calling GetLowestOfferings for LeftOutAsins: " + leftOutAsins + ", region: " + region);
                getLowestOfferingsCall.run();
                parseGetLowestOfferListingsResponse(getLowestOfferingsCall.getResponse(), lowestPriceMap);
            }

            if (log.isDebugEnabled()) {
                log.debug("LowestPrice obtained through getLowestOfferingsCall: " + lowestPriceMap);
            }

        } else {
            log.warn("GetLowestOfferListings is null for " + productIds);
        }

        if (response != null) {
            for (GetCompetitivePricingForASINResult result : response.getGetCompetitivePricingForASINResult()) {
                if (result.isSetStatus()) {
                    if ("success".equalsIgnoreCase(result.getStatus())) {
                        String asin = result.getASIN();
                        if (result.isSetProduct()) {
                            Product product = result.getProduct();
                            if (product.isSetCompetitivePricing()) {
                                CompetitivePricingType competitivePricing = product.getCompetitivePricing();
                                CompetitivePriceList compList = competitivePricing.getCompetitivePrices();
                                Float price = null;
                                for (CompetitivePriceType compPrice : compList.getCompetitivePrice()) {
                                    if (compPrice.isSetPrice()) {
                                        PriceType priceType = compPrice.getPrice();
                                        MoneyType moneyType = priceType.getListingPrice();
                                        BigDecimal amount = moneyType.getAmount();
                                        price = amount.floatValue();
                                        if (!lowestPriceMap.containsKey(asin)) {
                                            lowestPriceMap.put(asin, new HashMap<String, Pair<Float, Integer>>());
                                        }
                                        Map<String, Pair<Float, Integer>> priceQtyMap = lowestPriceMap.get(asin);
                                        if (!priceQtyMap.containsKey(compPrice.getCondition())) {
                                            priceQtyMap.put(compPrice.getCondition(), new Pair<Float, Integer>(price,
                                                    -1));
                                        } else {
                                            if (compPrice.isSetBelongsToRequester()
                                                    && !compPrice.isBelongsToRequester()) {
                                                priceQtyMap.get(compPrice.getCondition()).setFirst(
                                                        Math.min(priceQtyMap.get(compPrice.getCondition()).getFirst(),
                                                                price));
                                            } else {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("ASIN " + asin
                                                            + " already has the lowest price, ignoring this price.");
                                                }
                                            }
                                        }
                                    }
                                }
                                NumberOfOfferListingsList offerListings = competitivePricing.getNumberOfOfferListings();
                                Integer quantity = null;
                                for (OfferListingCountType offerListing : offerListings.getOfferListingCount()) {
                                    String condition = offerListing.getCondition();
                                    quantity = offerListing.getValue();
                                    if (!lowestPriceMap.containsKey(asin)) {
                                        lowestPriceMap.put(asin, new HashMap<String, Pair<Float, Integer>>());
                                    }
                                    Map<String, Pair<Float, Integer>> priceQtyMap = lowestPriceMap.get(asin);
                                    if (!priceQtyMap.containsKey(condition)) {
                                        priceQtyMap.put(condition, new Pair<Float, Integer>(-1F, quantity));
                                    } else {
                                        priceQtyMap.get(condition).setSecond(quantity);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("LowestPrice obtained through getCompetitive Price " + lowestPriceMap);
            }
        }
        return lowestPriceMap;
    }

    private Map<String, List<String>> parseGetLowestOfferListingsResponse(
            GetLowestOfferListingsForASINResponse getLowestResponse,
            Map<String, Map<String, Pair<Float, Integer>>> lowestPriceMap) {
        List<GetLowestOfferListingsForASINResult> lowestResults = getLowestResponse
                .getGetLowestOfferListingsForASINResult();
        Map<String, List<String>> leftOutAsins = new HashMap<String, List<String>>();
        for (GetLowestOfferListingsForASINResult result : lowestResults) {
            if (result.isSetStatus()) {
                if (result.getStatus().equalsIgnoreCase("success")) {
                    String asin = result.getASIN();
                    boolean usedItems = false;
                    boolean newItems = false;
                    boolean scannedAll = true;
                    if (!lowestPriceMap.containsKey(asin)) {
                        lowestPriceMap.put(asin, new HashMap<String, Pair<Float, Integer>>());
                    }
                    if (result.isSetAllOfferListingsConsidered()) {
                        scannedAll = result.isAllOfferListingsConsidered();
                    }
                    if (result.isSetProduct()) {
                        Product product = result.getProduct();
                        LowestOfferListingList offerList = product.getLowestOfferListings();
                        if (offerList.isSetLowestOfferListing()) {
                            for (LowestOfferListingType offerType : offerList.getLowestOfferListing()) {
                                PriceType priceType = offerType.getPrice();
                                QualifiersType qualifiers = offerType.getQualifiers();
                                if (qualifiers != null) {
                                    if (priceType != null) {
                                        String condition = qualifiers.getItemCondition();
                                        MoneyType moneyType = priceType.getListingPrice();
                                        if (moneyType != null) {
                                            if (condition != null) {
                                                Float price = moneyType.getAmount().floatValue();
                                                if (!lowestPriceMap.containsKey(asin)) {
                                                    lowestPriceMap.put(asin,
                                                            new HashMap<String, Pair<Float, Integer>>());
                                                }
                                                Map<String, Pair<Float, Integer>> priceQtyMap = lowestPriceMap
                                                        .get(asin);
                                                if (condition.equals("Used")) {
                                                    usedItems = true;
                                                }
                                                if (condition.equals("New")) {
                                                    newItems = true;
                                                }
                                                if (!priceQtyMap.containsKey(condition)) {
                                                    priceQtyMap.put(condition, new Pair<Float, Integer>(price, -1));
                                                } else {
                                                    priceQtyMap.get(condition).setFirst(
                                                            Math.min(priceQtyMap.get(condition).getFirst(), price));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!scannedAll) {
                        if (!usedItems) {
                            if (!leftOutAsins.containsKey("Used")) {
                                leftOutAsins.put("Used", new ArrayList<String>());
                            }
                            leftOutAsins.get("Used").add(asin);
                        }
                        if (!newItems) {
                            if (!leftOutAsins.containsKey("New")) {
                                leftOutAsins.put("New", new ArrayList<String>());
                            }
                            leftOutAsins.get("New").add(asin);
                        }
                    }
                }
            }

        }
        return leftOutAsins;

    }

    public static synchronized void initialize() throws Exception {
        if (!initialized) {
            String keysFile = AppConfig.getString("keys");
            FileInputStream keysFileStream = new FileInputStream(keysFile);
            try {
                Properties properties = new Properties();
                properties.load(keysFileStream);

                // Get Client for Product Service
                PRODUCT_SERVICE_MAP.put("US", getProductServiceClient("US", properties, false));
                PRODUCT_SERVICE_MAP.put("CA", getProductServiceClient("CA", properties, false));
                PRODUCT_SERVICE_MAP.put("UK", getProductServiceClient("UK", properties, false));
                PRODUCT_SERVICE_MAP.put("JP", getProductServiceClient("JP", properties, false));
                PRODUCT_SERVICE_MAP.put("ES", getProductServiceClient("ES", properties, false));
                PRODUCT_SERVICE_MAP.put("IT", getProductServiceClient("IT", properties, false));
                PRODUCT_SERVICE_MAP.put("DE", getProductServiceClient("DE", properties, false));
                PRODUCT_SERVICE_MAP.put("FR", getProductServiceClient("FR", properties, false));
                PRODUCT_SERVICE_MAP.put("CN", getProductServiceClient("CN", properties, false));

                // Get Client for Feed Service
                FEEDS_SERVICE_MAP.put("US", getFeedsServiceClient("US", properties));
                FEEDS_SERVICE_MAP.put("CA", getFeedsServiceClient("CA", properties));
                FEEDS_SERVICE_MAP.put("UK", getFeedsServiceClient("UK", properties));
                FEEDS_SERVICE_MAP.put("JP", getFeedsServiceClient("JP", properties));
                FEEDS_SERVICE_MAP.put("ES", getFeedsServiceClient("ES", properties));
                FEEDS_SERVICE_MAP.put("IT", getFeedsServiceClient("IT", properties));
                FEEDS_SERVICE_MAP.put("DE", getFeedsServiceClient("DE", properties));
                FEEDS_SERVICE_MAP.put("FR", getFeedsServiceClient("FR", properties));
                FEEDS_SERVICE_MAP.put("CN", getFeedsServiceClient("CN", properties));

                GET_WEIGHT_MONITOR.put("US", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("CA", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("UK", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("JP", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("ES", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("IT", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("DE", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("FR", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));
                GET_WEIGHT_MONITOR.put("CN", new AmazonAccessMonitor(20, GET_WEIGHT_RESTORE_RATE));

                GET_LOWEST_OFFER_MONITOR.put("US", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("CA", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("UK", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("JP", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("ES", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("IT", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("DE", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("FR", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_OFFER_MONITOR.put("CN", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));

                GET_LOWEST_PRICE_MONITOR.put("US", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("CA", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("UK", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("JP", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("ES", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("IT", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("DE", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("FR", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));
                GET_LOWEST_PRICE_MONITOR.put("CN", new AmazonAccessMonitor(20, GET_PRICE_RESTORE_RATE));

                initialized = true;
            } finally {
                if (keysFileStream != null) {
                    keysFileStream.close();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*
         * Merchant ID: A26M0U9VMRE4V5 Marketplace ID: ATVPDKIKX0DER
         */
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(1000));

        /**
         * Merchant ID: A2239UYQWWA9KK Marketplace ID: ATVPDKIKX0DER
         */
        final AmazonAccessMonitor monitor1 = new AmazonAccessMonitor(20, 10);

        int submitted = 0;
        long started = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            submitted++;
            executor.submit(new Test(i, monitor1));
        }
        while (executor.getCompletedTaskCount() < submitted) {
            Thread.sleep(100);
        }
        System.out.println("Time taken:" + (System.currentTimeMillis() - started));
        System.out.println("Process completed");
    }

    private static class Test implements Runnable {
        private int index;
        private AmazonAccessMonitor monitor;

        public Test(int i, AmazonAccessMonitor monitor) {
            index = i;
            this.monitor = monitor;
        }

        @Override
        public void run() {
            int waitTime = 0;
            // System.out.println(index + " " + new
            // Date(System.currentTimeMillis()) + " Added " + monitor);
            while ((waitTime = monitor.addRequest(3)) > 0) {
                // System.out.println(waitTime + " " + new
                // Date(System.currentTimeMillis()) + " " + index);
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {

                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(index + " " + new Date(System.currentTimeMillis()) + " completed " + monitor);
        }
    }

    private static class AmazonAccessMonitor {
        private int availableRequests;
        private int maxItemsPerSecond;
        private int restoreRatePerSecond;

        private long lastScheduled;
        private int offset = 0;

        public AmazonAccessMonitor(int maxItemsPerSecond, int restoreRatePerSecond) {
            this.maxItemsPerSecond = maxItemsPerSecond;
            this.restoreRatePerSecond = restoreRatePerSecond;
            this.availableRequests = this.maxItemsPerSecond;
        }

        private synchronized boolean canMakeRequest(int items) {
            long diff = System.currentTimeMillis() - lastScheduled + offset;
            if (lastScheduled > 0 && diff >= 1000) {
                long restored = restoreRatePerSecond * (diff / 1000);
                offset = (int) (diff % 1000);
                releaseRequest((int) restored);
            }
            if (availableRequests >= items) {
                return true;
            }
            return false;
        }

        public synchronized int addRequest(int items) {
            int waitTime = 0;
            if (canMakeRequest(items)) {
                releaseRequest(-1 * items);
            } else {
                waitTime = 350; // Wait for 350 ms
            }
            if (waitTime == 0) {
                lastScheduled = System.currentTimeMillis();
            }
            return waitTime;
        }

        private synchronized void releaseRequest(int items) {
            availableRequests = Math.min(maxItemsPerSecond, availableRequests + items);
        }

        public String toString() {
            return "av: " + availableRequests + " lastScheduled: " + lastScheduled % 1000000;
        }
    }

    private class GetLowestOfferingsThread implements Runnable {

        private boolean done = false;

        private GetLowestOfferListingsForASINResponse response;

        private List<String> products;

        private Map<String, List<String>> productCondition;

        private MarketplaceWebServiceProducts productService;

        private String condition = null;

        public GetLowestOfferingsThread(final List<String> products, final MarketplaceWebServiceProducts productService) {
            this.products = products;
            this.productService = productService;
        }

        public GetLowestOfferingsThread(final Map<String, List<String>> products,
                final MarketplaceWebServiceProducts productService) {
            this.productCondition = products;
            this.productService = productService;
            if (productCondition.containsKey("New")) {
                condition = "New";
                this.products = productCondition.get("New");
            } else if (productCondition.containsKey("Used")) {
                condition = "Used";
                this.products = productCondition.get("Used");
            } else {
                this.products = new ArrayList<String>();
            }
        }

        public void run() {

            try {

                AmazonAccessMonitor getLowestOfferMonitor = GET_LOWEST_OFFER_MONITOR.get(region);

                boolean shouldRetry = true;
                int size = products == null ? productCondition.size() : products.size();

                for (int retry = 0; shouldRetry && retry < MAX_RETRY; retry++) {
                    shouldRetry = false;
                    int tries = 0;
                    int waitTime = 0;
                    while (tries++ < MAX_TRIES) {
                        waitTime = getLowestOfferMonitor.addRequest(size);
                        if (waitTime == 0) {
                            break;
                        } else {
                            try {
                                Thread.sleep(waitTime);
                            } catch (InterruptedException e) {

                            }
                            log.debug("GetLowestOffer Waiting to get request quota.");
                        }
                    }
                    if (waitTime > 0) {
                        log.warn("A GetLowestOffer Request is starving for quota - executing without complying with policy.");
                    }

                    long startTime = System.currentTimeMillis();
                    GetLowestOfferListingsForASINRequest getLowestOfferListings = new GetLowestOfferListingsForASINRequest();
                    getLowestOfferListings.setExcludeMe(true);
                    getLowestOfferListings.setMarketplaceId(marketplaceId);
                    getLowestOfferListings.setSellerId(sellerId);
                    ASINListType asinList = new ASINListType(products);
                    if (condition != null) {
                        getLowestOfferListings.setItemCondition(condition);
                    }
                    getLowestOfferListings.setASINList(asinList);
                    try {
                        response = productService.getLowestOfferListingsForASIN(getLowestOfferListings);
                        if (log.isDebugEnabled()) {
                            log.info("Time taken for GetLowestOfferings call "
                                    + (System.currentTimeMillis() - startTime) + ", Region=" + region);
                        }
                    } catch (MarketplaceWebServiceProductsException e) {
                        if (e.getMessage().contains("throttled") || e.getMessage().contains("Throttled")) {
                            shouldRetry = true;
                            if (log.isDebugEnabled()) {
                                log.debug("Retrying throttled GetLowestOfferListing request for " + region);
                            }
                        }
                        log.error("GetLowestOfferListings error", e);
                    }
                }
            } finally {
                done = true;
            }

        }

        public boolean isDone() {
            return done;
        }

        public GetLowestOfferListingsForASINResponse getResponse() {
            return response;
        }
    }

    private class GetCompetitivePriceThread implements Runnable {

        private boolean done = false;

        private GetCompetitivePricingForASINResponse response;

        private List<String> products;

        private MarketplaceWebServiceProducts productService;

        public GetCompetitivePriceThread(final List<String> products, MarketplaceWebServiceProducts productService) {
            this.products = products;
            this.productService = productService;
        }

        public void run() {
            try {

                boolean shouldRetry = true;
                for (int retry = 0; shouldRetry && retry < MAX_RETRY; retry++) {
                    shouldRetry = false;
                    long startTime = System.currentTimeMillis();
                    int tries = 0;
                    int waitTime = 0;
                    AmazonAccessMonitor getLowestPriceMonitor = GET_LOWEST_PRICE_MONITOR.get(region);
                    while (tries++ < MAX_TRIES) {
                        waitTime = getLowestPriceMonitor.addRequest(products.size());
                        if (waitTime == 0) {
                            break;
                        } else {
                            try {
                                Thread.sleep(waitTime);
                            } catch (InterruptedException e) {
                                // wait..
                            }
                            log.debug("GetCompetitivePrice Waiting to get request quota.");
                        }
                    }
                    if (waitTime > 0) {
                        log.warn("A GetCompetitivePrice Request is starving for quota - executing without complying with policy.");
                    }

                    startTime = System.currentTimeMillis();
                    GetCompetitivePricingForASINRequest request = new GetCompetitivePricingForASINRequest();
                    request.setMarketplaceId(marketplaceId);
                    request.setSellerId(sellerId);

                    ASINListType asinList1 = new ASINListType(products);
                    request.setASINList(asinList1);
                    try {
                        response = productService.getCompetitivePricingForASIN(request);
                        if (log.isDebugEnabled()) {
                            log.debug("Time taken for GetCompetitivePrice call: "
                                    + (System.currentTimeMillis() - startTime) + ", Region=" + region);
                        }
                    } catch (MarketplaceWebServiceProductsException e) {
                        if (e.getMessage().contains("throttled") || e.getMessage().contains("Throttled")) {
                            shouldRetry = true;
                            if (log.isDebugEnabled()) {
                                log.debug("Retrying throttled GetCompetitivePrice request for " + region);
                            }
                        }
                        log.error("Error Getting Competitive Price.", e);
                    }
                }

            } finally {
                done = true;
            }
        }

        public boolean isDone() {
            return done;
        }

        public GetCompetitivePricingForASINResponse getResponse() {
            return response;
        }
    }

}

/*
 * <?xml version="1.0"?> <GetMatchingProductResponse
 * xmlns="http://mws.amazonservices.com/schema/Products/2011-10-01">
 * <GetMatchingProductResult ASIN="B002KT3XRQ" status="Success"> <Product
 * xmlns="http://mws.amazonservices.com/schema/Products/2011-10-01"
 * xmlns:ns2="http://mws.amazonservices.com/schema/Products/2011-10-01/default.xsd"
 * > <Identifiers> <MarketplaceASIN>
 * <MarketplaceId>ATVPDKIKX0DER</MarketplaceId> <ASIN>B002KT3XRQ</ASIN>
 * </MarketplaceASIN> </Identifiers> <AttributeSets> <ns2:ItemAttributes
 * xml:lang="en-US"> <ns2:Binding>Apparel</ns2:Binding> <ns2:Brand>Pearl
 * iZUMi</ns2:Brand> <ns2:Department>mens</ns2:Department> <ns2:Feature>Select
 * transfer fabric sets the benchmark for moisture transfer and four-way
 * performance stretch</ns2:Feature> <ns2:Feature>6-Panel anatomic design for
 * superior, chafe-free comfort, UPF 50+ sun protection</ns2:Feature>
 * <ns2:Feature>Comfortable silicone leg grippers keep shorts in place,
 * moisture-wicking, antimicrobial properties</ns2:Feature> <ns2:Feature>Tour 3D
 * Chamois is male-specific and non-chafing with padding in key
 * areas</ns2:Feature> <ns2:Feature>86 percent nylon, 14% spandex, 9-Inch
 * inseam</ns2:Feature> <ns2:ItemDimensions> <ns2:Height
 * Units="inches">2.00</ns2:Height> <ns2:Length Units="inches">9.00</ns2:Length>
 * <ns2:Width Units="inches">9.00</ns2:Width> </ns2:ItemDimensions>
 * <ns2:Label>Pearl iZUMi</ns2:Label> <ns2:ListPrice>
 * <ns2:Amount>50.00</ns2:Amount> <ns2:CurrencyCode>USD</ns2:CurrencyCode>
 * </ns2:ListPrice> <ns2:Manufacturer>Pearl iZUMi</ns2:Manufacturer>
 * <ns2:Model>0275</ns2:Model> <ns2:PackageDimensions> <ns2:Height
 * Units="inches">2.80</ns2:Height> <ns2:Length Units="inches">9.75</ns2:Length>
 * <ns2:Width Units="inches">8.40</ns2:Width> <ns2:Weight
 * Units="pounds">0.40</ns2:Weight> </ns2:PackageDimensions>
 * <ns2:PackageQuantity>1</ns2:PackageQuantity>
 * <ns2:PartNumber>0275</ns2:PartNumber>
 * <ns2:ProductGroup>Apparel</ns2:ProductGroup>
 * <ns2:ProductTypeName>SHORTS</ns2:ProductTypeName> <ns2:Publisher>Pearl
 * iZUMi</ns2:Publisher> <ns2:SmallImage>
 * <ns2:URL>http://ecx.images-amazon.com/images
 * /I/41ty3Sn%2BU8L._SL75_.jpg</ns2:URL> <ns2:Height
 * Units="pixels">75</ns2:Height> <ns2:Width Units="pixels">58</ns2:Width>
 * </ns2:SmallImage> <ns2:Studio>Pearl iZUMi</ns2:Studio> <ns2:Title>Pearl iZUMi
 * Men's Quest Cycling Short</ns2:Title> </ns2:ItemAttributes> </AttributeSets>
 * <Relationships> <ns2:VariationChild> <Identifiers> <MarketplaceASIN>
 * <MarketplaceId>ATVPDKIKX0DER</MarketplaceId> <ASIN>B002KT3XQC</ASIN>
 * </MarketplaceASIN> </Identifiers> <ns2:Color>Black</ns2:Color>
 * <ns2:Size>Small</ns2:Size> </ns2:VariationChild> <ns2:VariationChild>
 * <Identifiers> <MarketplaceASIN> <MarketplaceId>ATVPDKIKX0DER</MarketplaceId>
 * <ASIN>B002KT3XQW</ASIN> </MarketplaceASIN> </Identifiers>
 * <ns2:Color>Black</ns2:Color> <ns2:Size>Medium</ns2:Size>
 * </ns2:VariationChild> <ns2:VariationChild> <Identifiers> <MarketplaceASIN>
 * <MarketplaceId>ATVPDKIKX0DER</MarketplaceId> <ASIN>B002KT3XQM</ASIN>
 * </MarketplaceASIN> </Identifiers> <ns2:Color>Black</ns2:Color>
 * <ns2:Size>Large</ns2:Size> </ns2:VariationChild> <ns2:VariationChild>
 * <Identifiers> <MarketplaceASIN> <MarketplaceId>ATVPDKIKX0DER</MarketplaceId>
 * <ASIN>B002KT3XR6</ASIN> </MarketplaceASIN> </Identifiers>
 * <ns2:Color>Black</ns2:Color> <ns2:Size>X-Large</ns2:Size>
 * </ns2:VariationChild> <ns2:VariationChild> <Identifiers> <MarketplaceASIN>
 * <MarketplaceId>ATVPDKIKX0DER</MarketplaceId> <ASIN>B002KT3XRG</ASIN>
 * </MarketplaceASIN> </Identifiers> <ns2:Color>Black</ns2:Color>
 * <ns2:Size>XX-Large</ns2:Size> </ns2:VariationChild> </Relationships>
 * <SalesRankings> <SalesRank>
 * <ProductCategoryId>apparel_display_on_website</ProductCategoryId>
 * <Rank>159</Rank> </SalesRank> <SalesRank>
 * <ProductCategoryId>2420095011</ProductCategoryId> <Rank>1</Rank> </SalesRank>
 * <SalesRank> <ProductCategoryId>2611189011</ProductCategoryId> <Rank>4</Rank>
 * </SalesRank> </SalesRankings> </Product> </GetMatchingProductResult>
 * <ResponseMetadata>
 * <RequestId>b12caada-d330-4d87-a789-EXAMPLE35872</RequestId>
 * </ResponseMetadata> </GetMatchingProductResponse>
 */
