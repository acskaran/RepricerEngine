package kissmydisc.repricer.model;

import java.util.Date;

public class RepricerConfiguration {
    public long getCacheRefreshTime() {
        return cacheRefreshTime;
    }

    public void setCacheRefreshTime(long cacheRefreshTime) {
        this.cacheRefreshTime = cacheRefreshTime;
    }

    private String region;
    private String status;

    @Override
    public String toString() {
        return "RepricerConfiguration [region=" + region + ", status=" + status + ", nextRun=" + nextRun
                + ", interval=" + interval + ", formula=" + formula + ", sellerId=" + sellerId + ", marketplaceId="
                + marketplaceId + ", cacheRefreshTime=" + cacheRefreshTime + "]";
    }

    private Date nextRun;
    private int interval;
    private RepricerFormula formula;
    private String sellerId;
    private String marketplaceId;
    private long cacheRefreshTime;

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

    public Date getNextRun() {
        return nextRun;
    }

    public void setNextRun(Date nextRun) {
        this.nextRun = nextRun;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public RepricerFormula getFormula() {
        return formula;
    }

    public void setFormula(RepricerFormula formula) {
        this.formula = formula;
    }

    public String getSellerId() {
        return sellerId;
    }

    public void setSellerId(String sellerId) {
        this.sellerId = sellerId;
    }

    public String getMarketplaceId() {
        return marketplaceId;
    }

    public void setMarketplaceId(String marketplaceId) {
        this.marketplaceId = marketplaceId;
    }
}
