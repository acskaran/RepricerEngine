package kissmydisc.repricer.feeds;


public class PriceQuantityFeed implements AmazonFeed {
    private String sku;
    private float price;
    private int quantity;
    private String region;

    private static final String HEADER = "sku\tprice\tquantity";

    public PriceQuantityFeed(final String region) {
        this.region = region;
    }

    public static String getHeader() {
        return HEADER;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    private float round(float price) {
        price = Math.round(price + 0.5F);
        return price;
    }

    private String truncateDecimal(String price) {
        int d = price.indexOf('.');
        if (d == -1) {
            price += ".00";
        } else {
            d++; // converting to 1 based.
            int i = Math.min(d + 2, price.length());
            price = price.substring(0, i);
            if (i - d == 1) {
                price += "0";
            }
        }
        return price;
    }

    public String toString() {
        if (price > 0.0 && quantity >= 0) {
            String result = this.sku + "\t";
            String priceInStr = this.price + "";
            if (region.equals("JP")) {
                price = round(price);
                priceInStr = this.price + "";
                int index = priceInStr.indexOf(".");
                if (index == -1) {
                    index = priceInStr.length();
                }
                priceInStr = priceInStr.substring(0, index);
            } else {
                priceInStr = truncateDecimal(priceInStr);
                if (region.equals("IT") || region.equals("ES") || region.equals("DE") || region.equals("FR")) {
                    priceInStr = priceInStr.replace('.', ',');
                }
            }
            result += priceInStr + "\t";
            result += quantity;
            return result;
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        PriceQuantityFeed feed = new PriceQuantityFeed("JP");
        feed.setPrice(10.034F);
        feed.setQuantity(5);
        System.out.println(feed);

        feed = new PriceQuantityFeed("US");
        feed.setPrice(10.034F);
        feed.setQuantity(5);
        System.out.println(feed);

        feed = new PriceQuantityFeed("DE");
        feed.setPrice(10.034F);
        feed.setQuantity(5);
        System.out.println(feed);
    }
}
