package kissmydisc.repricer.model;

public class RepriceReport {
    private long repriceId;
    private long inventoryItemId;
    private String sku;
    private float price;
    private int quantity;
    private int formulaId;
    private String auditTrail;

    public long getInventoryItemId() {
        return inventoryItemId;
    }

    @Override
    public String toString() {
        return "RepriceReport [repriceId=" + repriceId + ", inventoryItemId=" + inventoryItemId + ", sku=" + sku
                + ", price=" + price + ", quantity=" + quantity + ", formulaId=" + formulaId + "]";
    }

    public void setInventoryItemId(long inventoryItemId) {
        this.inventoryItemId = inventoryItemId;
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

    public int getFormulaId() {
        return formulaId;
    }

    public void setFormulaId(int formulaId) {
        this.formulaId = formulaId;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public long getRepriceId() {
        return repriceId;
    }

    public void setRepriceId(long repriceId) {
        this.repriceId = repriceId;
    }

	public String getAuditTrail() {
		return auditTrail;
	}

	public void setAuditTrail(String auditTrail) {
		this.auditTrail = auditTrail;
	}
}
