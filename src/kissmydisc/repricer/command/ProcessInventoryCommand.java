package kissmydisc.repricer.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.InventoryFileDAO;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.model.InventoryFeed;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.utils.AppConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ProcessInventoryCommand extends Command {

    private String message;

    private long inventoryId = -1;

    private String inventoryRegion = null;

    private static final Log log = LogFactory.getLog(ProcessInventoryCommand.class);

    private static final int BATCH_SIZE = AppConfig.getInteger("PROCESS_INVENTORY_BATCH_SIZE", 100);

    public ProcessInventoryCommand(final int id, final String message, final Date date) {
        super(id, date);
        this.message = message;
        processMessage(message);
    }

    private ProcessInventoryCommand() {
        super(0, null);
    }

    private void processMessage(final String metadata) {
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("InventoryId=")) {
                inventoryId = Integer.parseInt(md.substring("InventoryId=".length()));
            }
            if (md.startsWith("InventoryRegion=")) {
                inventoryRegion = md.substring("InventoryRegion=".length());
            }
        }
    }

    @Override
    public void execute() {
        CommandDAO dao = new CommandDAO();
        try {
            dao.setStatus(getCommandId(), "STARTED");
            if (inventoryId == -1) {
                log.info("Invalid Process Inventory Command, " + this);
            } else {
                long time = System.currentTimeMillis();
                InventoryFileDAO ifDAO = new InventoryFileDAO();
                InventoryFeed feed = ifDAO.getInventoryFeed(inventoryId);
                long count = 0;
                if (feed != null) {
                    String filePath = downloads + File.separator + System.currentTimeMillis();
                    String downloadedFile = downloadFile(feed.getUrl(), filePath);
                    count = processContents(downloadedFile);
                    log.info("Processed the contents of " + feed.getUrl());
                    new File(downloadedFile).delete();
                    // Set the processed inventory as the latest inventory so
                    // the
                    // repricers can pick this inventory.

                    LatestInventoryDAO lidao = new LatestInventoryDAO();
                    lidao.setLatestInventory(inventoryRegion, inventoryId, count);
                    log.info("Updated the latest inventory for " + inventoryRegion + " as " + inventoryId);
                } else {
                    log.warn("Feed " + this + " not found");
                }

                long timeTaken = System.currentTimeMillis() - time;
                log.info("Time taken to process the inventory file " + this + " is " + timeTaken);
            }

            dao.setStatus(getCommandId(), "COMPLETED");
        } catch (Exception e) {
            try {
                log.error("Error processing the inventory file.", e);
                dao.setStatus(getCommandId(), "ERROR");
            } catch (Exception e1) {
                log.error("Unable to update the status as ERROR for " + this, e);
            }
        }
    }

    private Set<Integer> columnsNeeded = new HashSet<Integer>();

    private HashMap<Integer, String> columnAttributeMap = new HashMap<Integer, String>();

    Map<String, String> conversion = new HashMap<String, String>();

    private int findRequiredColumns(final String heading) {
        String[] columns = heading.split("\t");
        if (inventoryRegion.equals("US") || inventoryRegion.equals("UK") || inventoryRegion.equals("DE")
                || inventoryRegion.equals("IT") || inventoryRegion.equals("CA") || inventoryRegion.equals("ES")) {
            conversion.put("seller-sku", "sku");
            conversion.put("price", "price");
            conversion.put("quantity", "quantity");
            conversion.put("product-id", "product-id");
            conversion.put("item-condition", "item-condition");
            conversion.put("item-note", "item-note");
        } else if (inventoryRegion.equals("FR")) {
            conversion.put("sku-vendeur", "sku");
            conversion.put("prix", "price");
            conversion.put("quantit\u00e9", "quantity");
            conversion.put("r\u00e9f-produit", "product-id");
            conversion.put("\u00e9tat-article", "item-condition");
            conversion.put("remarque-article", "item-note");
        } else if (inventoryRegion.equals("JP")) {
            columnsNeeded.add(2);
            columnAttributeMap.put(2, "sku");
            columnsNeeded.add(3);
            columnAttributeMap.put(3, "price");
            columnsNeeded.add(4);
            columnAttributeMap.put(4, "quantity");
            columnsNeeded.add(10);
            columnAttributeMap.put(10, "product-id");
            columnsNeeded.add(8);
            columnAttributeMap.put(8, "item-condition");
            columnsNeeded.add(7);
            columnAttributeMap.put(7, "item-note");
        }
        if (!inventoryRegion.equals("JP")) {
            for (int i = 0; i < columns.length; i++) {
                if (conversion.containsKey(columns[i])) {
                    String converted = conversion.get(columns[i]);
                    columnsNeeded.add(i);
                    columnAttributeMap.put(i, converted);
                }
            }
        }
        return columns.length;
    }

    private long processContents(String downloadedFile) throws Exception {
        String charset = "ISO8859-1";
        if (inventoryRegion.equals("JP"))
            charset = "Shift_JIS";
        if (inventoryRegion.equals("FR"))
            charset = "ISO8859-1";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(downloadedFile), charset));
        try {
            String heading = reader.readLine();
            int reqdCols = findRequiredColumns(heading);
            String line = null;
            List<InventoryFeedItem> itemList = new ArrayList<InventoryFeedItem>();
            long processed = 0;
            while (true) {
                line = reader.readLine();
                if (line != null) {
                    String[] values = line.split("\t");
                    if (values != null && values.length != reqdCols) {
                        log.info("Row with wrong format " + line + ", ignoring..");
                    } else {
                        InventoryFeedItem item = new InventoryFeedItem();
                        item.setInventoryId(inventoryId);
                        item.setRegion(inventoryRegion);
                        String region_product_id = inventoryRegion;
                        boolean obi = false;
                        for (int col : columnsNeeded) {
                            if ("sku".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                item.setSku(values[col]);
                            } else if ("price".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                item.setPrice(Float.parseFloat(values[col]));
                            } else if ("quantity".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                item.setQuantity(Integer.parseInt(values[col]));
                            } else if ("product-id".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                item.setProductId(values[col]);
                                region_product_id += "_" + values[col];
                                item.setRegionProductId(region_product_id);
                            } else if ("item-condition".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                item.setCondition(Integer.parseInt(values[col]));
                            } else if ("item-note".equalsIgnoreCase(columnAttributeMap.get(col))) {
                                String val = values[col];
                                if (val.contains("OBI") || val.contains("Obi") || val.contains("obi")
                                        || val.contains("OBi") || val.contains("oBi")) {
                                    obi = true;
                                }
                            }
                        }
                        // ALTER TABLE `inventory_items`
                        // ADD COLUMN `obi` BIT NULL AFTER
                        // `lowest_amazon_price`;
                        if (obi && item.getCondition() != 11) {
                            item.setObiItem(true);
                        }
                        itemList.add(item);
                    }
                    processed++;
                }
                if (processed % 1000 == 0) {
                    log.info("Processed " + processed + " rows " + this);
                }
                if (itemList.size() == BATCH_SIZE || line == null || line == "") {
                    new InventoryItemDAO().addItems(itemList);
                    itemList = new ArrayList<InventoryFeedItem>();
                }

                if (line == null) { // Nothing to process after
                                    // this.
                    break;
                }
            }
            return processed;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    @Override
    public String toString() {
        return "ProcessInventory: [id=" + getCommandId() + ", message=" + message + ", date=" + getDate() + "]";
    }

    public static void main(String[] args) throws Exception {
        File file = new File("F:\\Documents\\Sridhar\\Programming\\Elance\\Repricer\\test.txt");
        FileInputStream fs = new FileInputStream(file);
        new ProcessInventoryCommand()
                .processContents("F:\\Documents\\Sridhar\\Programming\\Elance\\Repricer\\test.txt");
    }

}
