package kissmydisc.repricer.model;

public class AmazonSubmission {
    public String getSubmissionId() {
        return submissionId;
    }

    public void setSubmissionId(String submissionId) {
        this.submissionId = submissionId;
    }

    public long getSourceId() {
        return sourceId;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public AmazonSubmissionType getType() {
        return type;
    }

    public void setType(AmazonSubmissionType type) {
        this.type = type;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    private String submissionId;
    private long sourceId;
    private AmazonSubmissionType type;
    private String filePath;
    private String md5;

    public enum AmazonSubmissionType {
        PQ_FEED, INV_LOADER
    }
}
