package org.atlasapi.equiv;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Throwables;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public class S3Processor {

    private static final Logger log = LoggerFactory.getLogger(S3Processor.class);


    private final String s3Bucket;
    private final AmazonS3Client s3Client;

    private S3Processor(String s3Access, String s3Secret, String s3Bucket) {
        this.s3Bucket = s3Bucket;
        this.s3Client = new AmazonS3Client(
                new BasicAWSCredentials(
                        s3Access,
                        s3Secret
                ));
    }

    public static S3Processor create(String s3Access, String s3Secret, String s3Bucket) {
        return new S3Processor(s3Access, s3Secret, s3Bucket);
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public Optional<StoredEquivalenceResults> getStoredEquivalenceResults(String filePath) {
        S3Object s3object;
        try {
            s3object = s3Client.getObject(s3Bucket, filePath);
        } catch (Exception e) {
            return Optional.empty();
        }
        S3ObjectInputStream s3InputStream = s3object.getObjectContent();


        try(GZIPInputStream gzipInputStream = new GZIPInputStream(s3InputStream);
                ObjectInputStream objectInputStream = new ObjectInputStream(gzipInputStream)) {
            Object readObject = objectInputStream.readObject();
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
        log.info("Uploading file {} to S3 bucket {}", key, this.s3Bucket);
        s3Client.putObject(this.s3Bucket, key, file);
    }
}
