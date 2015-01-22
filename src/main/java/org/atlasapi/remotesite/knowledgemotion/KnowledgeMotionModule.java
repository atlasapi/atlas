package org.atlasapi.remotesite.knowledgemotion;

import javax.annotation.PostConstruct;

import org.atlasapi.googlespreadsheet.GoogleSpreadsheetModule;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.atlasapi.remotesite.knowledgemotion.topics.cache.KeyphraseTopicCache;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightKeywordsExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightResourceParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class KnowledgeMotionModule {

    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired GoogleSpreadsheetModule spreadsheet;

    private @Autowired DatabasedMongo mongo;

    /**
     * Here we wire what is in fact a {@link TopicCreatingTopicResolver}, so we may create new topics where necessary.
     */
    @Qualifier("topicStore")
    @Autowired
    private TopicStore topicStore;

    static final ImmutableList<KnowledgeMotionSourceConfig> SOURCES = ImmutableList.of(
//            KnowledgeMotionSourceConfig.from("GlobalImageworks", Publisher.KM_GLOBALIMAGEWORKS, "globalImageWorks:%s", "http://globalimageworks.com/%s"),
//            KnowledgeMotionSourceConfig.from("BBC Worldwide", Publisher.KM_BBC_WORLDWIDE, "km-bbcWorldwide:%s", "http://bbc.knowledgemotion.com/%s"),
//            KnowledgeMotionSourceConfig.from("British Movietone", Publisher.KM_MOVIETONE, "km-movietone:%s", "http://movietone.knowledgemotion.com/%s"),
            KnowledgeMotionSourceConfig.from("Bloomberg", Publisher.KM_BLOOMBERG, "bloomberg:%s", "http://bloomberg.com/%s")
    );

    static final ImmutableList<KnowledgeMotionSourceConfig> FIX_SOURCES = ImmutableList.of(
            KnowledgeMotionSourceConfig.from("Bloomberg", Publisher.KM_BLOOMBERG, "bloomberg:%s", "http://bloomberg.com/%s")
    );

    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(knowledgeMotionUpdater().withName("KnowledgeMotion Spreadsheet Updater"), RepetitionRules.NEVER);
        scheduler.schedule(knowledgeMotionBloombergIdFixer().withName("KnowledgeMotion Bloomberg ID Fixer"), RepetitionRules.NEVER);
    }

    @Bean
    public TopicGuesser topicGuesser() {
        return new TopicGuesser(
                new SpotlightKeywordsExtractor(new SpotlightResourceParser()),
                new KeyphraseTopicCache(mongo),
                topicStore
        );
    }

    private KnowledgeMotionUpdateTask knowledgeMotionUpdater() {
        return new KnowledgeMotionUpdateTask(SOURCES,
                spreadsheet.spreadsheetFetcher(), 
                new DefaultKnowledgeMotionDataRowHandler(contentResolver, contentWriter, new KnowledgeMotionDataRowContentExtractor(SOURCES, topicGuesser())),
                new KnowledgeMotionAdapter(),
                contentLister);
    }
    
    private KnowledgeMotionSpecialIdFixingTask knowledgeMotionBloombergIdFixer() {
        return new KnowledgeMotionSpecialIdFixingTask(FIX_SOURCES,
                spreadsheet.spreadsheetFetcher(), 
                new SpecialIdFixingKnowledgeMotionDataRowHandler(contentResolver, contentWriter, FIX_SOURCES.get(0)),
                new KnowledgeMotionAdapter(),
                contentLister);
    }

}
