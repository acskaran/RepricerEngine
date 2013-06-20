package kissmydisc.repricer.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.InventoryFileDAO;
import kissmydisc.repricer.dao.ProductDAO;
import kissmydisc.repricer.model.InventoryFeed;

public class AddAssociationCommand extends Command {

    private String region;

    private String url;

    private static final Log log = LogFactory.getLog(AddAssociationCommand.class);

    public AddAssociationCommand(final int id, final String message, final Date date) {
        super(id, date);
        String[] mds = message.split("\t");
        for (String md : mds) {
            if (md.startsWith("Region=")) {
                this.region = md.substring("Region=".length());
            }
            if (md.startsWith("URL=")) {
                url = md.substring("URL=".length());
            }
        }
    }

    @Override
    public void execute() {
        CommandDAO dao = new CommandDAO();
        try {
            dao.setStatus(getCommandId(), "STARTED");
            String filePath = downloads + File.separator + System.currentTimeMillis();
            String downloadedFile = downloadFile(url, filePath);
            processContents(downloadedFile, new ProductDAO());
            dao.setStatus(getCommandId(), "COMPLETED");
        } catch (Exception e) {
            try {
                log.error("Error processing AddAssociationFile.", e);
                dao.setStatus(getCommandId(), "ERROR");
            } catch (Exception e1) {
                log.error("Unable to update the status as ERROR for " + this, e);
            }
        }
    }

    private void processContents(String filePath, ProductDAO pdao) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        try {
            String line = null;
            do {
                line = reader.readLine();
                if (line != null) {
                    String[] values = line.trim().split("\t");
                    pdao.addAssociation(region, values[0], values[1]);
                }
            } while (line != null);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

}
