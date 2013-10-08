package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.atlasapi.s3.S3Client;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

public class DefaultAmazonUnboxFileStore implements AmazonUnboxFileStore {
    
    private static final String FILENAME_PATTERN = "Amazon_Unbox_Catalogue_%s.xml";
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date();
    
    private final Ordering<File> ORDER_BY_DATE = new Ordering<File>() {
        @Override
        public int compare(File left, File right) {
            LocalDate leftDate = extractDate(left.getName());
            LocalDate rightDate = extractDate(right.getName());
            return leftDate.compareTo(rightDate);
        }
        
        private LocalDate extractDate(String fileName) {
            String dateString = fileName.replace("Amazon_Unbox_Catalogue_", "").replace(".xml", "");
            return DATE_FORMATTER.parseLocalDate(dateString);
        }
    }; 
    
    private final File localFolder;
    private final S3Client s3client;

    public DefaultAmazonUnboxFileStore(String localFilesPath, S3Client s3client) {
        checkNotNull(localFilesPath);
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
    public void save(InputStream dataStream) throws IOException {
        FileOutputStream fos = null;
        try {
            String fileName = createFileName(new LocalDate(DateTimeZone.UTC));
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
    
    private String createFileName(LocalDate date) {
        return String.format(FILENAME_PATTERN, DATE_FORMATTER.print(date));
    }

    @Override
    public InputStream getLatestData() throws NoDataException {
        File latestFile = ORDER_BY_DATE.max(Arrays.asList(localFolder.listFiles()));
        if (latestFile == null) {
            throw new NoDataException("No files found");
        }
        try {
            return new FileInputStream(latestFile);
        } catch (FileNotFoundException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
    }
}
