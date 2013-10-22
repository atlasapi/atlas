package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.atlasapi.s3.S3Client;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

public class DefaultAmazonUnboxFileStore implements AmazonUnboxFileStore {
    
    private final File localFolder;
    private final S3Client s3client;
    private final Ordering<File> fileOrdering;

    public DefaultAmazonUnboxFileStore(String localFilesPath, Ordering<File> fileOrdering, S3Client s3client) {
        checkNotNull(localFilesPath);
        this.fileOrdering = checkNotNull(fileOrdering);
        this.s3client = checkNotNull(s3client);
        this.localFolder = new File(localFilesPath);
        if (!localFolder.exists()) {
            localFolder.mkdir();
        }
        if (!localFolder.isDirectory()) {
            throw new IllegalArgumentException("Files path is not a directory: " + localFilesPath);
        }
    }
  
    @Override
    public void save(String fileName, InputStream dataStream) throws IOException {
        FileOutputStream fos = null;
        try {
            File localFile = new File(localFolder, fileName);
            fos = new FileOutputStream(localFile);
            IOUtils.copy(dataStream, fos);
            s3client.put(fileName, localFile);
        } finally {
            if (fos != null) {
                fos.close();
            }
        }
    }
    
    @Override
    public InputStream getLatestData() throws NoDataException, IOException {
        File latestFile = fileOrdering.max(Arrays.asList(localFolder.listFiles()));
        if (latestFile == null) {
            throw new NoDataException("No files found");
        }
        try {
            ZipFile zipFile = new ZipFile(latestFile.getAbsolutePath());
            ZipEntry fileData = zipFile.entries().nextElement();
            return zipFile.getInputStream(fileData);
        } catch (FileNotFoundException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
    }
}
