package org.atlasapi.remotesite.itunes;

import java.io.File;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.itunes.epf.ItunesEpfUpdateTask;
import org.atlasapi.remotesite.itunes.epf.LatestEpfDataSetSupplier;
import org.atlasapi.remotesite.util.OldContentDeactivator;
import org.atlasapi.s3.DefaultS3Client;

import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.SimpleScheduler;

import com.google.common.base.Strings;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.metabroadcast.common.scheduling.RepetitionRules.daily;
import static org.atlasapi.persistence.logging.AdapterLogEntry.infoEntry;

@Configuration
public class ItunesModule {

    private final static RepetitionRule ITUNES_EPF_UPDATER = daily(new LocalTime(5, 0, 0));
    private final static RepetitionRule ITUNES_FILE_UPDATER = daily(new LocalTime(4, 0, 0));

    private @Value("${itunes.epf.username}") String epfUsername;
    private @Value("${itunes.epf.password}") String epfPassword;
    private @Value("${itunes.epf.feedPath}") String feedPath;
    private @Value("${itunes.epf.localFilesPath}") String localFilesPath;
    private @Value("${itunes.s3.bucket}") String s3bucket;
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;

    @Autowired private SimpleScheduler scheduler;
    @Autowired private ContentWriter contentWriter;
    @Autowired private ContentResolver contentResolver;
    @Autowired private ContentLister contentLister;
    @Autowired private AdapterLog log;

    @PostConstruct
    public void startBackgroundTasks() {
        checkArgument(!Strings.isNullOrEmpty(localFilesPath));
        try {
            scheduler.schedule(
                    itunesFileUpdater().withName("iTunes File Updater"),
                    ITUNES_FILE_UPDATER
            );

            log.record(infoEntry()
                    .withDescription("iTunes File update task installed (%s)", localFilesPath)
                    .withSource(getClass())
            );

            scheduler.schedule(
                    new ItunesEpfUpdateTask(
                            new LatestEpfDataSetSupplier(new File(localFilesPath)),
                            contentDeactivator(),
                            contentWriter,
                            log
                    ).withName("iTunes EPF Updater"),
                    ITUNES_EPF_UPDATER
            );

            log.record(infoEntry()
                    .withDescription("iTunes EPF update task installed (%s)", localFilesPath)
                    .withSource(getClass())
            );
        } catch (Exception e) {
            log.record(infoEntry()
                    .withDescription("iTunes EPF update task installed failed")
                    .withSource(getClass())
                    .withCause(e));
        }
    }

    private OldContentDeactivator contentDeactivator() {
        return OldContentDeactivator.create(contentLister, contentWriter, contentResolver);
    }

    @Bean
    ItunesFileUpdater itunesFileUpdater() {
        return ItunesFileUpdater.create(epfFileUpdater());
    }

    @Bean
    ItunesEpfFileUpdater epfFileUpdater() {
        return ItunesEpfFileUpdater.builder()
                .withUsername(epfUsername)
                .withPassword(epfPassword)
                .withFeedPath(feedPath)
                .withLocalFilesPath(localFilesPath)
                .withS3Client(new DefaultS3Client(s3access, s3secret, s3bucket))
                .build();
    }
}
