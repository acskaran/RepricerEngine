package kissmydisc.repricer.command;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;

import kissmydisc.repricer.dao.DBAccessor;

public abstract class Command extends DBAccessor {

	private int commandId;

	private Date date;
	
	protected static String downloads = "downloads";

    static {
        new File(downloads).mkdir();
    }

    protected static String downloadFile(String url, String filePath) throws IOException {
        InputStream is = null;
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(filePath));
        try {
            is = new URL(url).openStream();
            int len;
            byte buf[] = new byte[4096];
            while ((len = is.read(buf)) != -1) {
                os.write(buf, 0, len);
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (os != null) {
                os.close();
            }
        }

        return filePath;
    }

	public Command(final int id, final Date date) {
		this.commandId = id;
		this.date = date;
	}

	public abstract void execute();

	public int getCommandId() {
		return commandId;
	}

	public Date getDate() {
		return date;
	}

}
