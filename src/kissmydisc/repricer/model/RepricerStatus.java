package kissmydisc.repricer.model;

import java.util.Date;

public class RepricerStatus {

	private long start = System.currentTimeMillis();

	public long getRepriceId() {
		return repriceId;
	}

	public void setRepriceId(long repriceId) {
		this.repriceId = repriceId;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setLastRepriced(String lastRepriced) {
		this.lastRepriced = lastRepriced;
	}

	public void setLastRepricedCheckPoint(int lastRepricedCheckPoint) {
		this.lastRepricedId = lastRepricedCheckPoint;
	}

	public int getTotalScheduled() {
		return totalScheduled;
	}

	public void setTotalScheduled(int totalScheduled) {
		this.totalScheduled = totalScheduled;
	}

	public int getTotalCompleted() {
		return totalCompleted;
	}

	public void setTotalCompleted(int totalCompleted) {
		this.totalCompleted = totalCompleted;
	}

	public int getTotalRepriced() {
		return totalRepriced;
	}

	public void setTotalRepriced(int totalRepriced) {
		this.totalRepriced = totalRepriced;
	}

	public float getRepriceRate() {
		return (totalCompleted * 1.0F * 1000.0F / getElapsed()) * 3600.0F;
	}

	public void setRepriceRate(float repriceRate) {
		this.repriceRate = repriceRate;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public int getQuantityReset() {
		return quantityResetToZero;
	}

	public int getPriceUp() {
		return priceUp;
	}

	public int getPriceDown() {
		return priceDown;
	}

	public String getLastRepriced() {
		return lastRepriced;
	}

	@Override
	public String toString() {
		return "RepricerStatus [start=" + start + ", repriceId=" + repriceId
				+ ", region=" + region + ", status=" + status
				+ ", totalScheduled=" + totalScheduled + ", totalCompleted="
				+ totalCompleted + ", totalRepriced=" + totalRepriced
				+ ", repriceRate=" + repriceRate + ", startTime=" + startTime
				+ ", endTime=" + endTime + ", quantityResetToZero="
				+ quantityResetToZero + ", priceDown=" + priceDown
				+ ", priceUp=" + priceUp + ", noPriceChange=" + noPriceChange
				+ ", lastRepriced=" + lastRepriced + ", lastRepricedId="
				+ lastRepricedId + ", elapsed=" + elapsed + ", lowestPrice="
				+ lowestPrice + "]";
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public synchronized void addOneCompleted() {
		this.totalCompleted++;
	}

	public synchronized void addScheduled(int n) {
		this.totalScheduled += n;
	}

	public synchronized void addOneRepriced() {
		this.totalRepriced++;
	}

	public enum METRIC {
		PRICE_UP, QUANTITY_RESET_TO_ZERO, PRICE_DOWN, SAME_PRICE, OBI_QUANTITY_RESET_TO_ZERO, OBI_PRICE_UP, OBI_PRICE_DOWN, OBI_SAME_PRICE
	}

	public synchronized void addRepriceMetrics(final String lastRepriced,
			long id, METRIC metric, boolean lowestPrice) {
		if (this.lastRepricedId < id) {
			this.lastRepriced = lastRepriced;
			this.lastRepricedId = id;
		}
		if (metric.equals(METRIC.PRICE_DOWN)) {
			this.priceDown++;
		} else if (metric.equals(METRIC.PRICE_UP)) {
			this.priceUp++;
		} else if (metric.equals(METRIC.QUANTITY_RESET_TO_ZERO)) {
			this.quantityResetToZero++;
		} else if (metric.equals(METRIC.SAME_PRICE)) {
			this.noPriceChange++;
		} else if (metric.equals(METRIC.OBI_PRICE_DOWN)) {
			this.setObiPriceDown(this.getObiPriceDown() + 1);
		} else if (metric.equals(METRIC.OBI_PRICE_UP)) {
			this.setObiPriceUp(this.getObiPriceUp() + 1);
		} else if (metric.equals(METRIC.OBI_QUANTITY_RESET_TO_ZERO)) {
			this.setObiQuantityResetToZero(this.getObiQuantityResetToZero() + 1);
		} else if (metric.equals(METRIC.OBI_SAME_PRICE)) {
			this.setObiNoPriceChange(this.getObiNoPriceChange() + 1);
		}
		if (lowestPrice) {
			this.lowestPrice++;
		}
	}

	public long getElapsed() {
		long currentTime = System.currentTimeMillis();
		elapsed += (currentTime - start);
		start = currentTime;
		return elapsed;
	}

	public void setElapsed(long elapsed) {
		this.elapsed = elapsed;
	}

	private long repriceId;
	private String region;
	private String status;
	private int totalScheduled;
	private int totalCompleted;
	private int totalRepriced;
	private float repriceRate;
	private Date startTime;
	private Date endTime;
	private int quantityResetToZero = 0;
	private int priceDown = 0;
	private int priceUp = 0;
	private int noPriceChange = 0;
	private String lastRepriced = null;
	private long lastRepricedId = 0;
	private long elapsed = 0;
	private int lowestPrice = 0;
	private int obiPriceUp = 0;
	private int obiPriceDown = 0;
	private int obiNoPriceChange = 0;
	private int obiQuantityResetToZero = 0;

	public int getQuantityResetToZero() {
		return quantityResetToZero;
	}

	public void setQuantityResetToZero(int quantityResetToZero) {
		this.quantityResetToZero = quantityResetToZero;
	}

	public long getLastRepricedId() {
		return lastRepricedId;
	}

	public void setLastRepricedId(int lastRepricedId) {
		this.lastRepricedId = lastRepricedId;
	}

	public void setPriceDown(int priceDown) {
		this.priceDown = priceDown;
	}

	public void setPriceUp(int priceUp) {
		this.priceUp = priceUp;
	}

	public int getLowestPrice() {
		return lowestPrice;
	}

	public void setLowestPrice(int lowestPrice) {
		this.lowestPrice = lowestPrice;
	}

	public int getNoPriceChange() {
		return noPriceChange;
	}

	public void setNoPriceChange(int noPriceChange) {
		this.noPriceChange = noPriceChange;
	}

	public int getObiPriceDown() {
		return obiPriceDown;
	}

	public void setObiPriceDown(int obiPriceDown) {
		this.obiPriceDown = obiPriceDown;
	}

	public int getObiPriceUp() {
		return obiPriceUp;
	}

	public void setObiPriceUp(int obiPriceUp) {
		this.obiPriceUp = obiPriceUp;
	}

	public int getObiNoPriceChange() {
		return obiNoPriceChange;
	}

	public void setObiNoPriceChange(int obiNoPriceChange) {
		this.obiNoPriceChange = obiNoPriceChange;
	}

	public int getObiQuantityResetToZero() {
		return obiQuantityResetToZero;
	}

	public void setObiQuantityResetToZero(int obiQuantityResetToZero) {
		this.obiQuantityResetToZero = obiQuantityResetToZero;
	}

}
