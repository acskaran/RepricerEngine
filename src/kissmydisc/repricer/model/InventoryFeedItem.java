package kissmydisc.repricer.model;

public class InventoryFeedItem {
	private long inventoryItemId;
	private String sku;
	private String productId;
	private int quantity;
	private float price;
	private int condition;
	private long inventoryId;
	private String region;
	private String region_product_id; // Used to do prefix queries.
	private float weight;
	private float lowestAmazonPrice;
	private boolean obiItem = false;
	private long itemNo = 0;
	
	public boolean getObiItem() {
		return obiItem;
	}
	
	public void setItemNo(long itemNo) {
	    this.itemNo = itemNo;
	}
	
	public long getItemNo() {
	    return this.itemNo;
	}

	public void setObiItem(boolean obi) {
		this.obiItem = obi;
	}

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

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public int getCondition() {
		return condition;
	}

	public void setCondition(int condition) {
		this.condition = condition;
	}

	public long getInventoryId() {
		return inventoryId;
	}

	public void setInventoryId(long inventoryId) {
		this.inventoryId = inventoryId;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public void setRegionProductId(String region_product_id) {
		this.region_product_id = region_product_id;
	}

	public String getRegionProductId() {
		return region_product_id;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public float getLowestAmazonPrice() {
		return lowestAmazonPrice;
	}

	public void setLowestAmazonPrice(float lowestAmazonPrice) {
		this.lowestAmazonPrice = lowestAmazonPrice;
	}

	@Override
	public String toString() {
		return "InventoryFeedItem [sku=" + sku + ", productId=" + productId
				+ ", quantity=" + quantity + ", price=" + price
				+ ", condition=" + condition + ", inventoryId=" + inventoryId
				+ ", region=" + region + ", region_product_id="
				+ region_product_id + ", weight=" + weight
				+ ", lowestAmazonPrice=" + lowestAmazonPrice + "]";
	}

	public long getInventoryItemId() {
		return inventoryItemId;
	}

	public void setInventoryItemId(long inventoryItemId) {
		this.inventoryItemId = inventoryItemId;
	}
}
