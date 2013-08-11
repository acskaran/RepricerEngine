package kissmydisc.repricer.command;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.CreateListingStatusDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.engine.ListingCopier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AddItemsToInventoryAsyncCommand implements Runnable {

    private String fromRegion;

    private String toRegion;

    private int id;

    private static Log log = LogFactory.getLog(AddItemsToInventoryAsyncCommand.class);

    public AddItemsToInventoryAsyncCommand(int id, String fromRegion, String toRegion) {
        this.id = id;
        this.fromRegion = fromRegion;
        this.toRegion = toRegion;
    }

    public void run() {
        CommandDAO cdao = new CommandDAO();
        try {
            cdao.setStatus(id, "STARTED");
            if (fromRegion != null && toRegion != null) {
                new CreateListingStatusDAO().updateStatus(toRegion, "STARTED", "UPLOADING_INVENTORY");
                new ListingCopier(fromRegion, toRegion).copyListings();
                new CreateListingStatusDAO().completeCreateListing(toRegion);
            }
            cdao.setStatus(id, "COMPLETED");
        } catch (Exception e) {
            log.error("Error copying listings.", e);
            try {
                cdao.setStatus(id, "ERROR");
                new CreateListingStatusDAO().updateStatus(toRegion, "ERROR");
            } catch (Exception e1) {
                // TODO Auto-generated catch block
            }
        }
    }

}
