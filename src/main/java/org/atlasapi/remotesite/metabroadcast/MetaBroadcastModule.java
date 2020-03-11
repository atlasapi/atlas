package org.atlasapi.remotesite.metabroadcast;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.net.HostSpecifier;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.text.ParseException;

import static org.atlasapi.persistence.MongoModule.OWL_DATABASED_MONGO;
import static org.atlasapi.remotesite.HttpClients.webserviceClient;

@Configuration
public class MetaBroadcastModule {
    
    private @Value("${cannon.host.name}") String cannonHostName;
    private @Value("${cannon.host.port}") Integer cannonHostPort;
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;
    private @Value("${magpie.s3.bucket}") String s3Bucket;
    private @Value("${magpie.s3.folder}") String s3folder;
    
    private @Autowired @Qualifier(OWL_DATABASED_MONGO) DatabasedMongo mongo;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentWriter contentWriter;
    private @Autowired @Qualifier("topicStore") TopicStore topicStore;
    private @Autowired @Qualifier("topicQueryResolver") TopicQueryResolver topicResolver;
    private @Autowired SimpleScheduler scheduler;
    private @Autowired AdapterLog log;
    
    private static final String CONTENT_WORDS_FOR_PROGRAMMES = "contentWords";
    private static final String CONTENT_WORDS_LIST_FOR_PROGRAMMES = "contentWordsList";
    private static final String CONTENT_WORDS_FOR_AUDIENCE = "contentWordsForPeopleTalk";
    private static final String CONTENT_WORDS_LIST_FOR_AUDIENCE = "contentWordsListForPeopleTalk";
    
    @PostConstruct
    public void scheduleTasks() {
//        scheduler.schedule(twitterUpdaterTask().withName("Voila Twitter topics ingest"), RepetitionRules.every(Duration.standardHours(12)).withOffset(Duration.standardHours(7)));
//        scheduler.schedule(twitterPeopleTalkUpdaterTask().withName("Voila Twitter audience topics ingest"), RepetitionRules.every(Duration.standardHours(3)));
        scheduler.schedule(magpieUpdaterTask().withName("Magpie ingest"), RepetitionRules.daily(new LocalTime(3, 0 , 0)));
    }

    @Bean
    CannonTwitterTopicsUpdater twitterUpdaterTask() {
        return new CannonTwitterTopicsUpdater(cannonTopicsClient(), 
                new MetaBroadcastTwitterTopicsUpdater(cannonTopicsClient(), contentResolver, topicStore, topicResolver, contentWriter, MetaBroadcastTwitterTopicsUpdater.TWITTER_NS_FOR_AUDIENCE, log));
    }
    
    @Bean
    MagpieUpdaterTask magpieUpdaterTask() {
        return new MagpieUpdaterTask(magpieResultsSource(), magpieUpdater(), new MongoSchedulingStore(mongo));
    }
    
    @Bean
    RemoteMagpieResultsSource magpieResultsSource() {
        return new S3MagpieResultsSource(awsService(), s3Bucket, s3folder);
    }

    @Bean
    CannonTwitterTopicsUpdater twitterPeopleTalkUpdaterTask() {
        return new CannonTwitterTopicsUpdater(cannonTopicsClient(), 
                new MetaBroadcastTwitterTopicsUpdater(cannonPeopleTalkClient(), contentResolver, topicStore, topicResolver, contentWriter, MetaBroadcastTwitterTopicsUpdater.TWITTER_NS_FOR_AUDIENCE_RELATED, log));
    }
    
    
@Bean
    MetaBroadcastMagpieUpdater magpieUpdater() {
        return new MetaBroadcastMagpieUpdater(contentResolver, topicStore, 
                topicResolver, contentWriter);
    }

    @Bean 
    CannonTwitterTopicsClient cannonTopicsClient() {
        try {
            return new CannonTwitterTopicsClient(webserviceClient(), HostSpecifier.from(cannonHostName), Optional.fromNullable(cannonHostPort), log, CONTENT_WORDS_LIST_FOR_PROGRAMMES, CONTENT_WORDS_FOR_PROGRAMMES);
        } catch (ParseException e) {
            throw Throwables.propagate(e);
        }
    }
    
    @Bean 
    CannonTwitterTopicsClient cannonPeopleTalkClient() {
        try {
            return new CannonTwitterTopicsClient(webserviceClient(), HostSpecifier.from(cannonHostName), Optional.fromNullable(cannonHostPort), log, CONTENT_WORDS_LIST_FOR_AUDIENCE, CONTENT_WORDS_FOR_AUDIENCE);
        } catch (ParseException e) {
            throw Throwables.propagate(e);
        }
    }
    
    @Bean
    S3Service awsService() {
        try {
            return new RestS3Service(new AWSCredentials(s3access, s3secret));
        } catch (S3ServiceException e) {
            throw Throwables.propagate(e);
        }
    }
    
}
