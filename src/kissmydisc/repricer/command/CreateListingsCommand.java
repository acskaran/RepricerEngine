package kissmydisc.repricer.command;

import java.util.Date;

import kissmydisc.repricer.RepricerMainThreadPool;

public class CreateListingsCommand extends Command {

    private String region;

    private String fileUrl;
    
    private boolean continueCreateListing = false;

    public CreateListingsCommand(int id, String metadata, Date date) {
        super(id, date);
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("Region=")) {
                this.region = md.substring("Region=".length());
            }
            if (md.startsWith("File=")) {
                this.fileUrl = md.substring("File=".length());
            }
            if (md.startsWith("Continue=")) {
                this.continueCreateListing = true;
            }
        }
    }

    @Override
    public void execute() {
        if (fileUrl == null || fileUrl == "") {
            RepricerMainThreadPool.getInstance().submit(new CreateListingsAsyncCommand(getCommandId(), region, this.continueCreateListing));
        } else {
            RepricerMainThreadPool.getInstance().submit(new CreateListingsAsyncCommand(getCommandId(), region, fileUrl, this.continueCreateListing));
        }
    }

}
