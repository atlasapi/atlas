package org.atlasapi.remotesite.rt;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

public class RtFilmModule {

    private final static Daily DAILY = RepetitionRules.daily(new LocalTime(5, 30, 0));

    @Value("${pa.film.feedUrl}")
    private String feedUrl;

    @Autowired private SimpleScheduler scheduler;
    @Autowired private AdapterLog log;
    @Autowired private ContentWriter contentWriter;
    @Autowired private ContentResolver contentResolver;
    @Autowired private ItemsPeopleWriter peopleWriter;

    @PostConstruct
    public void startUp() {
        scheduler.schedule(rtFilmFeedDeltaUpdater().withName("RT Film Feed Updater"), DAILY);
        scheduler.schedule(rtFilmFeedCompleteUpdater().withName("RT Film Feed Complete Updater"), RepetitionRules.NEVER);
    }

    @Bean
    public RtFilmFeedUpdater rtFilmFeedDeltaUpdater() {
        return new RtFilmFeedUpdater(feedUrl, log, contentResolver, contentWriter, rtFilmProcessor());
    }
    
    @Bean
    public RtFilmFeedUpdater rtFilmFeedCompleteUpdater() {
        return RtFilmFeedUpdater.completeUpdater(feedUrl, log, contentResolver, contentWriter, rtFilmProcessor());
    }
    
    @Bean
    public RtFilmProcessor rtFilmProcessor() {
        return new RtFilmProcessor(contentResolver, contentWriter, peopleWriter, log);
    }
}
