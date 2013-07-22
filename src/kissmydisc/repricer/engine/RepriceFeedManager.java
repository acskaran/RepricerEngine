package kissmydisc.repricer.engine;

import java.util.ArrayList;

import kissmydisc.repricer.model.PriceQuantityFeed;

public interface RepriceFeedManager {
    public void writeToFeedFile(PriceQuantityFeed feed) throws Exception;
    public void flush() throws Exception;
    public void close() throws Exception;
    
}
