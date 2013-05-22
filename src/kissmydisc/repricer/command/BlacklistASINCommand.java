package kissmydisc.repricer.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kissmydisc.repricer.dao.CommandDAO;
import kissmydisc.repricer.dao.DBException;
import kissmydisc.repricer.dao.ProductDAO;

public class BlacklistASINCommand extends Command {

    private String region;

    private String url;

    private static final Log log = LogFactory.getLog(BlacklistASINCommand.class);

    public BlacklistASINCommand(int id, String metadata, Date date) {
        super(id, date);
        String[] mds = metadata.split("\t");
        for (String md : mds) {
            if (md.startsWith("Region=")) {
                region = md.substring("Region=".length());
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
            if (region == null || url == null) {
                log.info("Invalid Process Inventory Command, " + this);
            } else {
                ProductDAO pdao = new ProductDAO();
                pdao.whitelist(region);
                String filePath = downloads + File.separator + System.currentTimeMillis();
                String downloadedFile = downloadFile(url, filePath);
                processContents(downloadedFile, pdao);
            }
            dao.setStatus(getCommandId(), "COMPLETED");
        } catch (Exception e) {
            log.error("Error blacklisting", e);
            try {
                dao.setStatus(getCommandId(), "ERROR");
            } catch (DBException e1) {
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
                    String productId = line.trim();
                    pdao.addToBlacklist(region, productId);
                }
            } while (line != null);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

}
