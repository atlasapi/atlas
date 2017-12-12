package org.atlasapi.s3;

import com.metabroadcast.common.base.Maybe;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

public class DefaultS3Client implements S3Client {

    private final String access;
    private final String secret;
    private final S3Bucket bucket;

    public DefaultS3Client(String access, String secret, String bucketName) {
        this.access = access;
        this.secret = secret;
        this.bucket = new S3Bucket(bucketName);
    }

    private RestS3Service client() {
        try {
            return new RestS3Service(new AWSCredentials(access, secret));
        } catch (S3ServiceException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String name, File file) throws IOException {
        S3Object put = null;
        try {
            put = new S3Object(file);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        put.setAcl(AccessControlList.REST_CANNED_PRIVATE);
        put.setLastModifiedDate(new DateTime().toDate());
        try {
            client().putObject(bucket, put);
        } catch (S3ServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean getAndSaveIfUpdated(String name, File fileToWrite, Maybe<File> existingFile) throws IOException {
        S3Object s3object = null;
        try {
            s3object = client().getObject(bucket, name);
        } catch (S3ServiceException e) {
            if (!"NoSuchKey".equals(e.getS3ErrorCode())) {
                throw new IOException(e);
            }
        }
        
        if (s3object == null) {
            return false;
        }
        
        try {
            if (existingFile.isNothing()
                    || (s3object.getContentLength() == existingFile.requireValue().length() && s3object.getLastModifiedDate().before(new Date(existingFile.requireValue().lastModified())))) {
                InputStream is = s3object.getDataInputStream();
                try {
                    FileUtils.writeLines(fileToWrite, IOUtils.readLines(is));
                } finally {
                    IOUtils.closeQuietly(is);
                }
            }
        } catch (ServiceException e) {
            throw new IOException(e);
        }
        
        return true;
    }
}
