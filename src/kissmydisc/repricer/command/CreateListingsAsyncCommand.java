package kissmydisc.repricer.command;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.CreateListingStatusDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.InventoryItemDAO;
import kissmydisc.repricer.dao.LatestInventoryDAO;
import kissmydisc.repricer.dao.ListingConfigurationDAO;
import kissmydisc.repricer.dao.RepricerConfigurationDAO;
import kissmydisc.repricer.dao.RepricerStatusReportDAO;
import kissmydisc.repricer.engine.CreateListingsWorker;
import kissmydisc.repricer.model.InventoryFeedItem;
import kissmydisc.repricer.model.ListingConfiguration;
import kissmydisc.repricer.model.RepricerConfiguration;
import kissmydisc.repricer.model.RepricerStatus;
import kissmydisc.repricer.utils.AppConfig;
import kissmydisc.repricer.utils.Pair;
import kissmydisc.repricer.utils.PriceUtils;

public class CreateListingsAsyncCommand implements Runnable {

    private String region;

    private int id;

    private String fileUrl;

    private static Log log = LogFactory.getLog(CreateListingsAsyncCommand.class);

    public CreateListingsAsyncCommand(final int id, final String region) {
        this.id = id;
        this.region = region;
    }

    public CreateListingsAsyncCommand(int id, String region, String fileUrl) {
        this.id = id;
        this.region = region;
        this.fileUrl = fileUrl;
    }

    public void run() {
        CommandDAO cdao = new CommandDAO();
        try {
            cdao.setStatus(id, "STARTED");
            new CreateListingStatusDAO().startCreateListing(region);
            if (region != null) {
                if (fileUrl == null) {
                    new CreateListingStatusDAO().updateStatus(region, "STARTED", "SCANNING DB");
                    new CreateListingsWorker(region).createListings();
                } else {
                    new CreateListingStatusDAO().updateStatus(region, "STARTED", "SCANNING FILE");
                    new CreateListingsWorker(region, fileUrl).createListings();
                }
                String command = "CONTINUE_REPRICER";
                String newRegion = "N-" + this.region;
                RepricerStatus status = new RepricerStatusReportDAO().addNewRepricer(newRegion);
                long repriceId = status.getRepriceId();
                new RepricerConfigurationDAO().setStatus(newRegion, "PAUSED", repriceId);
                String metadata = "Region=" + newRegion;
                int commandId = new CommandDAO().addCommand(command, metadata, "NEW");
                new CreateListingStatusDAO().updateStatus(region, "ISSUED", "REPRICE_COMMAND");
                boolean success = true;
                while (true) {
                    // wait for the command to complete.
                    kissmydisc.repricer.model.Command c = new CommandDAO().getCommand(commandId);
                    if (c != null && (c.getStatus().equals("COMPLETED") || c.getStatus().equals("ERROR"))) {
                        if (c.getStatus().equals("ERROR")) {
                            success = false;
                        }
                        break;
                    }
                    log.info("Waiting for command to complete commandId: " + commandId);
                    Thread.sleep(10000);
                }
                if (success) {
                    new CreateListingStatusDAO().updateStatus(region, "STARTED", "REPRICE_COMMAND");
                    log.info("Create Listings successful for " + region + ", started repricer successfully.");
                    while (true) {
                        RepricerConfiguration config = new RepricerConfigurationDAO().getRepricer(newRegion);
                        if (config != null
                                && (config.getStatus().equals("COMPLETED") || config.getStatus().equals("SCHEDULED") || config
                                        .getStatus().equals("TERMINATED"))) {
                            if (!(config.getStatus().equals("COMPLETED") || config.getStatus().equals("SCHEDULED"))) {
                                success = false;
                            }
                            break;
                        }
                        log.info("Waiting for reprice to complete " + config.getRegion() + ", " + config.getStatus());
                        Thread.sleep(60000);
                    }
                    if (success) {
                        new CreateListingStatusDAO().updateStatus(region, "STARTED", "CREATE_PREVIEW");
                        createPreviewFile(newRegion, region);
                        new CreateListingStatusDAO().updateStatus(region, "WAITING", "VERIFY_PREVIEW");
                    } else {
                        new CreateListingStatusDAO().updateStatus(region, "TERMINATED", "CREATE_LISTING");
                    }
                }
            }
            cdao.setStatus(id, "COMPLETED");
        } catch (Exception e) {
            log.error("Error creating new listings.", e);
            try {
                cdao.setStatus(id, "ERROR");
                new CreateListingStatusDAO().updateStatus(region, "ERROR");
            } catch (Exception e1) {
                // TODO Auto-generated catch block
            }
        }
    }

    private void createPreviewFile(String fromRegion, String toRegion) throws Exception {
        InventoryItemDAO inventoryDAO = new InventoryItemDAO();
        int limit = 1000;
        String moreToken = null;
        Pair<Long, Long> latestInventory = new LatestInventoryDAO().getLatestInventoryWithCount(fromRegion);
        Pair<List<InventoryFeedItem>, String> itemsAndMoreToken = inventoryDAO.getMatchingItems(
                latestInventory.getFirst(), fromRegion, moreToken, limit);
        moreToken = itemsAndMoreToken.getSecond();
        List<InventoryFeedItem> items = itemsAndMoreToken.getFirst();
        ListingConfiguration config = new ListingConfigurationDAO().getListingConfiguration(toRegion);
        String TAB = "\t";
        String NEWLINE = "\n";
        String previewFile = AppConfig.getString("PreviewFileLocation");
        previewFile += region;
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(previewFile));
        String header = config.getHeader() + NEWLINE;
        bos.write(header.getBytes());
        for (InventoryFeedItem item : items) {
            int parameter = 1;
            String listing = "";
            if (item.getQuantity() != 0) {
                while (true) {
                    String param = config.getNext(parameter++);
                    if (param == null)
                        break;
                    if (param.equals("sku")) {
                        listing += item.getSku() + TAB;
                    }
                    if (param.equals("product-id")) {
                        listing += item.getProductId() + TAB;
                    }
                    if (param.equals("product-id-type")) {
                        listing += "1" + TAB;
                    }
                    if (param.equals("item-condition")) {
                        listing += item.getCondition() + TAB;
                    }
                    if (param.equals("price")) {
                        listing += PriceUtils.getPrice(item.getPrice(), region) + TAB;
                    }
                    if (param.equals("quantity")) {
                        listing += item.getQuantity() + TAB;
                    }
                    if (param.equals("item-note")) {
                        if (item.getCondition() == 11) {
                            listing += config.getItemNoteNew() + TAB;
                        }
                        if (item.getCondition() == 2) {
                            if (item.getObiItem()) {
                                listing += config.getItemNoteObi() + TAB;
                            } else {
                                listing += config.getItemNoteUsed() + TAB;
                            }
                        }
                    }
                    if (param.equals("add-delete")) {
                        listing += "a" + TAB;
                    }
                    if (param.equals("will-ship-internationally")) {
                        String val = config.getWillShipInternationally();
                        if (val != null && val.trim() != "") {
                            listing += val.trim() + TAB;
                        }
                    }
                    if (param.equals("expedited-shipping")) {
                        String val = config.getExpeditedShipping();
                        if (val != null && val.trim() != "") {
                            listing += val.trim() + TAB;
                        }
                    }
                    if (param.equals("item-is-marketplace")) {
                        String val = config.getItemIsMarketplace();
                        if (val != null && val.trim() != "") {
                            listing += val.trim() + TAB;
                        }
                    }
                }
                listing = listing.trim();
                listing += NEWLINE;
                bos.write(listing.getBytes());
            }
        }
        bos.close();
    }
}