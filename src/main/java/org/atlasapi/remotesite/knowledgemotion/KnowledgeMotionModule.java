package org.atlasapi.remotesite.knowledgemotion;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.atlasapi.remotesite.knowledgemotion.topics.cache.KeyphraseTopicCache;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightKeywordsExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightResourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.metabroadcast.common.ingest.IngestService;
import com.metabroadcast.common.ingest.s3.process.FileProcessor;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

@Configuration
public class KnowledgeMotionModule {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeMotionModule.class);

    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired DatabasedMongo mongo;

    @Value("${km.contentdeals.aws.accessKey}")
    private String awsAccessKey;
    @Value("${km.contentdeals.aws.secretKey}")
    private String awsSecretKey;
    @Value("${km.contentdeals.aws.s3BucketName}")
    private String awsS3BucketName;

    /**
     * Here we wire what is in fact a {@link TopicCreatingTopicResolver}, so we may create new topics where necessary.
     */
    @Qualifier("topicStore")
    @Autowired
    private TopicStore topicStore;

    @PostConstruct
    public void start() {
        log.info("Initializing Knowledgemotion updater");
        AWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        IngestService ingestService = new IngestService(awsCredentials);
        FileProcessor fileProcessor = new KnowledgeMotionFileProcessor(contentResolver,
            contentWriter, contentLister, topicGuesser(), new KnowledgeMotionCsvTranslator());
        ingestService.registerFileProcessor(awsS3BucketName, fileProcessor);
        ingestService.start();
    }

    @Bean
    public TopicGuesser topicGuesser() {
        return new TopicGuesser(
                new SpotlightKeywordsExtractor(new SpotlightResourceParser()),
                new KeyphraseTopicCache(mongo),
                topicStore
        );
    }

}
