package kissmydisc.repricer.command;

import java.util.Date;

import kissmydisc.repricer.RepricerMainThreadPool;

public class AddItemsToInventoryCommand extends Command {

    private String fromRegion;

    private String toRegion;

    public AddItemsToInventoryCommand(int id, String metadata, Date date) {
        super(id, date);
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("fromRegion=")) {
                this.fromRegion = md.substring("fromRegion=".length());
            }
            if (md.startsWith("toRegion=")) {
                this.toRegion = md.substring("toRegion=".length());
            }
        }
    }

    @Override
    public void execute() {
        RepricerMainThreadPool.getInstance().submit(
                new AddItemsToInventoryAsyncCommand(getCommandId(), fromRegion, toRegion));
    }

}
