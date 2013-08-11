package kissmydisc.repricer.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FeedsDAO extends DBAccessor {

    public void addNewFeedSubmission(final long repriceId, final String feedFile, final String submissionId)
            throws Exception {
        String insertQuery = "insert into feed_submissions (REPRICE_ID, FEED_FILE, AMAZON_SUBMISSION_ID) values (?, ?, ?)";
        PreparedStatement st = null;
        Connection conn = null;
        try {
            conn = getConnection();
            st = conn.prepareStatement(insertQuery);
            st.setLong(1, repriceId);
            st.setString(2, feedFile);
            st.setString(3, submissionId);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e);
        } finally {
            releaseStatement(st);
            releaseConnection();
        }
    }
    
}
