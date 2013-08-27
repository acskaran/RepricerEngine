package kissmydisc.repricer.model;

public class InvalidItem {

    private String sku;

    private String productId;

    private int itemCondition = -1;

    private String region;

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getItemCondition() {
        return itemCondition;
    }

    public void setItemCondition(int itemCondition) {
        this.itemCondition = itemCondition;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
