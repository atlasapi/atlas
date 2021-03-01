package org.atlasapi.equiv;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class S3Processor {

    private static final Logger log = LoggerFactory.getLogger(S3Processor.class);


    private final AmazonS3Client s3Client;
    private final String s3BucketDownload;
    private final String s3BucketUpload;
    private final AwsConfiguration awsConfiguration;

    private S3Processor() {
        this.awsConfiguration = AwsConfiguration.create();
        this.s3Client = awsConfiguration.getS3Client();
        this.s3BucketDownload = awsConfiguration.getS3BucketDownload();
        this.s3BucketUpload = awsConfiguration.getS3BucketUpload();
    }

    public static S3Processor create() {
        return new S3Processor();
    }

    public String getS3BucketUpload() {
        return s3BucketUpload;
    }

    public Optional<StoredEquivalenceResults> getStoredEquivalenceResults(String filePath) {
        S3Object s3object;
        try {
            s3object = s3Client.getObject(s3BucketDownload, filePath);
        } catch (Exception e) {
            return Optional.empty();
        }
        S3ObjectInputStream s3InputStream = s3object.getObjectContent();


        try(ObjectInputStream is = new ObjectInputStream(s3InputStream)) {
            Object readObject = is.readObject();
            try {
                return Optional.of((StoredEquivalenceResults) readObject);
            } catch (ClassCastException e) { //Stored object might be the old result object
                return Optional.of(new StoredEquivalenceResults((StoredEquivalenceResult) readObject));
            }
        } catch (IOException | ClassNotFoundException e) {
            Throwables.propagate(e);
        }
        return Optional.empty();
    }

    public void uploadFile(String key, File file) {
        log.info("Uploading file {} to S3 bucket {}", key, this.s3BucketUpload);
        s3Client.putObject(this.s3BucketUpload, key, file);
    }
}
