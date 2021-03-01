package org.atlasapi.equiv;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class AwsConfiguration {

    @Value("${s3.access}") private final String s3Access;
    @Value("${s3.secret}") private final String s3Secret;
    @Value("${equiv.s3.bucket.download}") private final String s3BucketDownload;
    @Value("${equiv.s3.bucket.upload}") private final String s3BucketUpload;
    private final AmazonS3Client s3Client;

    private AwsConfiguration() {
        this.s3Access = getS3Access();
        this.s3Secret = getS3Secret();
        this.s3BucketDownload = getS3BucketDownload();
        this.s3BucketUpload = getS3BucketUpload();
        this.s3Client = new AmazonS3Client(
                new BasicAWSCredentials(
                        this.s3Access,
                        this.s3Secret
                ));
    }

    public static AwsConfiguration create() {
        return new AwsConfiguration();
    }

    public String getS3Access() {
        return s3Access;
    }

    public String getS3Secret() {
        return s3Secret;
    }

    public String getS3BucketDownload() {
        return s3BucketDownload;
    }

    public String getS3BucketUpload() {
        return s3BucketUpload;
    }

    public AmazonS3Client getS3Client() {
        return s3Client;
    }

}
