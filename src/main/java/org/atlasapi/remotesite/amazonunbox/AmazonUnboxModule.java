package org.atlasapi.remotesite.amazonunbox;

import java.io.File;
import java.util.Comparator;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.lovefilm.RestS3ServiceSupplier;
import org.atlasapi.s3.DefaultS3Client;
import org.atlasapi.s3.S3Client;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class AmazonUnboxModule {

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date();
    
    private final static Daily DAILY = RepetitionRules.daily(new LocalTime(4, 30, 0));
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired AdapterLog log;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired ContentResolver resolver;
    
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;
    private @Value("${unbox.s3.bucket}") String fileStoreS3Bucket;
    private @Value("${unbox.remote.s3.access}") String remoteS3Access;
    private @Value("${unbox.remote.s3.secret}") String remoteS3Secret;
    private @Value("${unbox.remote.s3.bucket}") String remoteS3Bucket;
    private @Value("${unbox.remote.s3.fileNamePrefix}") String remoteS3FileNamePrefix;
    private @Value("${unbox.path}") String unboxPath;
    private @Value("${unbox.missingContent.percentage}") Integer missingContentPercentage;
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(amazonUnboxUpdater().withName("Amazon Unbox Daily Updater"), DAILY);
    }

    private AmazonUnboxUpdateTask amazonUnboxUpdater() {
        S3Client s3Client = new DefaultS3Client(s3access, s3secret, fileStoreS3Bucket);
        AmazonUnboxFileStore fileStore = new DefaultAmazonUnboxFileStore(unboxPath, fileOrdering(), s3Client);
        
        AWSCredentials credentials = new AWSCredentials(remoteS3Access, remoteS3Secret);
        RestS3ServiceSupplier serviceSupplier = new RestS3ServiceSupplier(credentials);
        AmazonUnboxFileUpdater updater = new S3AmazonUnboxFileUpdater(serviceSupplier, remoteS3Bucket, s3FileOrdering(), fileStore);

        AmazonUnboxPreProcessingItemProcessor preProcessor = new AmazonUnboxPreProcessingItemProcessor();
        
        ContentExtractor<AmazonUnboxItem,Optional<Content>> contentExtractor = new AmazonUnboxContentExtractor();
        AmazonUnboxItemProcessor processor = new AmazonUnboxContentWritingItemProcessor(contentExtractor, resolver, contentWriter, contentLister, missingContentPercentage, preProcessor);
        
        return new AmazonUnboxUpdateTask(updater, fileStore, preProcessor, processor);
    }
    
    private Ordering<S3Object> s3FileOrdering() {
        return new Ordering<S3Object>() {
            @Override
            public int compare(S3Object left, S3Object right) {
                return ComparisonChain.start()
                        .compareTrueFirst(left.getName().contains(remoteS3FileNamePrefix), right.getName().contains(remoteS3FileNamePrefix))
                        .compare(left.getName(), right.getName(), fileNameByDateComparator())
                        .result();
            }
        };
        
    }

    private Ordering<File> fileOrdering() {
        return new Ordering<File>() {
            @Override
            public int compare(File left, File right) {
                return fileNameByDateComparator().compare(left.getName(), right.getName());
            }        
        };
    }
    
    private Comparator<String> fileNameByDateComparator() {
        return new Comparator<String>() {
            @Override
            public int compare(String left, String right) {
                LocalDate leftDate = extractDate(left);
                LocalDate rightDate = extractDate(right);
                return leftDate.compareTo(rightDate);
            }
        };
    }
    
    private LocalDate extractDate(String fileName) {
        String dateString = fileName.replace(remoteS3FileNamePrefix, "").replace(".xml.zip", "");
        return DATE_FORMATTER.parseLocalDate(dateString);
    }
}
