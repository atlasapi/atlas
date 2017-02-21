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
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

import com.google.common.base.Strings;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.atlasapi.persistence.logging.AdapterLogEntry.infoEntry;

@Configuration
//@Import({ItunesAdapterModule.class})
public class ItunesModule {

    private final static Daily FIVE_AM = RepetitionRules.daily(new LocalTime(5, 0, 0));
    private final static Daily FOUR_AM = RepetitionRules.daily(new LocalTime(4, 0, 0));

    private @Value("${itunes.epf.username}") String epfUsername;
    private @Value("${itunes.epf.password}") String epfPassword;
    private @Value("${itunes.epf.feedPath}") String feedPath;
    private @Value("${itunes.epf.localFilesPath}") String localFilesPath;
    private @Value("${itunes.s3.bucket}") String s3bucket;
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;


    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentLister contentLister;
    private @Autowired AdapterLog log;

    //    private @Autowired ItunesBrandAdapter itunesBrandAdapter;
    //
    //    private Set<String> feeds = ImmutableSet.of("http://ax.itunes.apple.com/WebObjects/MZStoreServices.woa/ws/RSS/toptvseasons/sf=143444/limit=300/xml",
    //                                                "http://ax.itunes.apple.com/WebObjects/MZStoreServices.woa/ws/RSS/toptvepisodes/sf=143444/limit=300/xml");
    //
    //    @PostConstruct
    //    public void startBackgroundTasks() {
    //        //scheduler.schedule(itunesRssUpdater(), AT_NIGHT);
    //        log.record(new AdapterLogEntry(Severity.INFO).withDescription("iTunes update scheduled task installed").withSource(ItunesRssUpdater.class));
    //    }
    //
    //    public @Bean ItunesRssUpdater itunesRssUpdater() {
    //        return new ItunesRssUpdater(feeds, HttpClients.webserviceClient(), contentWriter, itunesBrandAdapter, log);
    //    }
    //
    @PostConstruct
    public void startBackgroundTasks() {
        try {
            scheduler.schedule(
                    itunesFileUpdater().withName("iTunes File Updater"),
                    FOUR_AM
            );

            if (!Strings.isNullOrEmpty(localFilesPath)) {
                scheduler.schedule(
                        new ItunesEpfUpdateTask(
                                new LatestEpfDataSetSupplier(new File(localFilesPath)),
                                contentDeactivator(),
                                contentWriter,
                                log
                        ).withName("iTunes EPF Updater"), FIVE_AM);

                log.record(infoEntry()
                        .withDescription("iTunes EPF update task installed (%s)", localFilesPath)
                        .withSource(getClass())
                );
            } else {
                log.record(infoEntry()
                        .withDescription("iTunes EPF update task not installed", localFilesPath)
                        .withSource(getClass())
                );
            }
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
