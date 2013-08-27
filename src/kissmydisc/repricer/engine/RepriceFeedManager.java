package kissmydisc.repricer.engine;

import java.util.List;

import kissmydisc.repricer.feeds.AmazonFeed;
import kissmydisc.repricer.model.AmazonSubmission;

public interface RepriceFeedManager {

    public void writeToFeedFile(AmazonFeed feed) throws Exception;

    public void flush() throws Exception;

    public List<AmazonSubmission> getSubmissions();
}
