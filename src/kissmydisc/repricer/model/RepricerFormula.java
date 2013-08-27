package kissmydisc.repricer.model;

public class RepricerFormula {
    private int formulaId;
    private String initialFormula;
    private double defaultWeight;
    private double defaultObiWeight;
    private int quantityLimit;
    private boolean usedFilter;
    private boolean secondLevelRepricing;
    private double secondLevelRepricingUpperLimitPercent;
    private double secondLevelRepricingUpperLimit;
    private double secondLevelRepricingLowerLimit;
    private double secondLevelRepricingLowerLimitPercent;
    private double lowerPriceMarigin;
    private String obiFormula;
    private int obiQuantityLimit;
    private int newQuantityLimit;

    public String getInitialFormula() {
        return initialFormula;
    }

    public void setInitialFormula(String initialFormula) {
        this.initialFormula = initialFormula;
    }

    public int getQuantityLimit() {
        return quantityLimit;
    }

    public int getNewQuantityLimit() {
        return newQuantityLimit;
    }

    public void setQuantityLimit(int quantityLimit) {
        this.quantityLimit = quantityLimit;
    }

    public boolean isUsedFilter() {
        return usedFilter;
    }

    public void setUsedFilter(boolean usedFilter) {
        this.usedFilter = usedFilter;
    }

    public int getFormulaId() {
        return formulaId;
    }

    public void setFormulaId(int formulaId) {
        this.formulaId = formulaId;
    }

    public boolean isSecondLevelRepricing() {
        return secondLevelRepricing;
    }

    public void setSecondLevelRepricing(boolean secondLevelRepricing) {
        this.secondLevelRepricing = secondLevelRepricing;
    }

    public double getSecondLevelRepricingUpperLimitPercent() {
        return secondLevelRepricingUpperLimitPercent;
    }

    public void setSecondLevelRepricingUpperLimitPercent(double secondLevelRepricingUpperLimitPercent) {
        this.secondLevelRepricingUpperLimitPercent = secondLevelRepricingUpperLimitPercent;
    }

    public double getSecondLevelRepricingUpperLimit() {
        return secondLevelRepricingUpperLimit;
    }

    public void setSecondLevelRepricingUpperLimit(double secondLevelRepricingUpperLimit) {
        this.secondLevelRepricingUpperLimit = secondLevelRepricingUpperLimit;
    }

    public double getSecondLevelRepricingLowerLimit() {
        return secondLevelRepricingLowerLimit;
    }

    public void setSecondLevelRepricingLowerLimit(double secondLevelRepricingLowerLimit) {
        this.secondLevelRepricingLowerLimit = secondLevelRepricingLowerLimit;
    }

    public double getSecondLevelRepricingLowerLimitPercent() {
        return secondLevelRepricingLowerLimitPercent;
    }

    public void setSecondLevelRepricingLowerLimitPercent(double secondLevelRepricingLowerLimitPercent) {
        this.secondLevelRepricingLowerLimitPercent = secondLevelRepricingLowerLimitPercent;
    }

    public double getLowerPriceMarigin() {
        return lowerPriceMarigin;
    }

    public void setLowerPriceMarigin(double lowerPriceMarigin) {
        this.lowerPriceMarigin = lowerPriceMarigin;
    }

    public String getObiFormula() {
        return obiFormula;
    }

    public void setObiFormula(String obiFormula) {
        this.obiFormula = obiFormula;
    }

    public int getObiQuantityLimit() {
        return obiQuantityLimit;
    }

    public void setObiQuantityLimit(int obiQuantityLimit) {
        this.obiQuantityLimit = obiQuantityLimit;
    }

    @Override
    public String toString() {
        return "RepricerFormula [formulaId=" + formulaId + ", initialFormula=" + initialFormula + ", defaultWeight="
                + defaultWeight + ", defaultObiWeight=" + defaultObiWeight + ", quantityLimit=" + quantityLimit
                + ", usedFilter=" + usedFilter + ", secondLevelRepricing=" + secondLevelRepricing
                + ", secondLevelRepricingUpperLimitPercent=" + secondLevelRepricingUpperLimitPercent
                + ", secondLevelRepricingUpperLimit=" + secondLevelRepricingUpperLimit
                + ", secondLevelRepricingLowerLimit=" + secondLevelRepricingLowerLimit
                + ", secondLevelRepricingLowerLimitPercent=" + secondLevelRepricingLowerLimitPercent
                + ", lowerPriceMarigin=" + lowerPriceMarigin + ", obiFormula=" + obiFormula + ", obiQuantityLimit="
                + obiQuantityLimit + ", newQuantityLimit=" + newQuantityLimit + "]";
    }

    public double getDefaultWeight() {
        return defaultWeight;
    }

    public void setDefaultWeight(double defaultWeight) {
        this.defaultWeight = defaultWeight;
    }

    public double getDefaultObiWeight() {
        return defaultObiWeight;
    }

    public void setDefaultObiWeight(double defaultObiWeight) {
        this.defaultObiWeight = defaultObiWeight;
    }

    public void setQuantityLimitNew(int qtyLimit) {
        this.newQuantityLimit = qtyLimit;
    }
}
