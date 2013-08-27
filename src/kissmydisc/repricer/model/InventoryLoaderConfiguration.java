package kissmydisc.repricer.model;

import java.util.HashMap;
import java.util.Map;

import kissmydisc.repricer.utils.Constants;

public class InventoryLoaderConfiguration {
    private String region;
    private String itemNoteNew;
    private String itemNoteUsed;
    private String itemNoteObi;
    private String willShipInternationally;
    private String expeditedShipping;
    private String itemIsMarketplace;

    private int quantity;
    private String addDelete;

    public String getWillShipInternationally() {
        return willShipInternationally;
    }

    public void setWillShipInternationally(String willShipInternationally) {
        this.willShipInternationally = willShipInternationally;
    }

    public String getExpeditedShipping() {
        return expeditedShipping;
    }

    public void setExpeditedShipping(String expeditedShipping) {
        this.expeditedShipping = expeditedShipping;
    }

    public String getItemIsMarketplace() {
        return itemIsMarketplace;
    }

    public void setItemIsMarketplace(String itemIsMarketplace) {
        this.itemIsMarketplace = itemIsMarketplace;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getItemNoteNew() {
        return itemNoteNew;
    }

    public void setItemNoteNew(String itemNoteNew) {
        this.itemNoteNew = itemNoteNew;
    }

    public String getItemNoteUsed() {
        return itemNoteUsed;
    }

    public void setItemNoteUsed(String itemNoteUsed) {
        this.itemNoteUsed = itemNoteUsed;
    }

    public String getItemNoteObi() {
        return itemNoteObi;
    }

    public void setItemNoteObi(String itemNoteObi) {
        this.itemNoteObi = itemNoteObi;
    }

    private static Map<Integer, String> map = new HashMap<Integer, String>();

    public String getHeader() {
        String header = "";
        for (int i = 1; i < 9; i++) {
            if (i != 1) {
                header += Constants.TAB;
            }
            header += map.get(i);
        }
        for (int i = 9; i <= map.size(); i++) {
            String heading = map.get(i);
            String value = null;
            if (heading == "will-ship-internationally" && willShipInternationally != null
                    && willShipInternationally.trim() != "") {
                value = willShipInternationally;
            }
            if (heading == "item-is-marketplace" && itemIsMarketplace != null && itemIsMarketplace.trim() != "") {
                value = itemIsMarketplace;
            }
            if (heading == "expedited-shipping" && expeditedShipping != null && expeditedShipping.trim() != "") {
                value = expeditedShipping;
            }
            if (value != null) {
                header += Constants.TAB + heading;
            }
        }
        header = header.trim();
        return header;
    }

    public String getNext(int iter) {
        if (map.containsKey(iter)) {
            return map.get(iter);
        }
        return null;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getAddDelete() {
        return addDelete;
    }

    public void setAddDelete(String addDelete) {
        this.addDelete = addDelete;
    }

    static {
        map.put(1, "sku");
        map.put(2, "product-id");
        map.put(3, "product-id-type");
        map.put(4, "item-condition");
        map.put(5, "price");
        map.put(6, "quantity");
        map.put(7, "add-delete");
        map.put(8, "item-note");
        map.put(9, "will-ship-internationally");
        map.put(10, "item-is-marketplace");
        map.put(11, "expedited-shipping");
    }

}
