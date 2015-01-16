package org.atlasapi.remotesite.bt.channels;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.GsonBtMpxClient;
import org.atlasapi.remotesite.pa.PaModule;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Import( PaModule.class )
public class BtChannelsModule {

    private static final String ALIAS_NAMESPACE_PREFIX = "bt";
    private static final String URI_PREFIX_STRING_FORMAT = "http://%s/";

    private static final RepetitionRule PROD_INGEST_REPETITION = RepetitionRules.every(Duration.standardHours(2));
    private static final LocalTime NON_PROD_BASE_START_TIME = new LocalTime(16,0);
    
    private static final RepetitionRule TEST1_INGEST_REPETITION = RepetitionRules.daily(NON_PROD_BASE_START_TIME);
    private static final RepetitionRule TEST2_INGEST_REPETITION = RepetitionRules.daily(NON_PROD_BASE_START_TIME.plusHours(1));
    private static final RepetitionRule REFERENCE_INGEST_REPETITION = RepetitionRules.daily(NON_PROD_BASE_START_TIME.plusHours(2));
    
    @Value("${bt.channels.baseUri.production}")
    private String baseUri;
    
    @Value("${bt.channels.baseUri.test1}")
    private String test1BaseUri;
    
    @Value("${bt.channels.baseUri.test2}")
    private String test2BaseUri;
    
    @Value("${bt.channels.baseUri.reference}")
    private String test3BaseUri;
    
    @Value("${bt.channels.freeviewPlatformChannelGroupId}")
    private String freeviewPlatformChannelGroupId;
    
    @Autowired
    private ChannelGroupResolver channelGroupResolver;
    
    @Autowired
    private ChannelGroupWriter channelGroupWriter;
    
    @Autowired
    private ChannelResolver channelResolver;
    
    @Autowired
    private ChannelWriter channelWriter;
    
    @Autowired
    private SimpleScheduler scheduler;
    
    @Autowired
    private ReentrantLock channelWriterLock;
    
    @Bean
    public BtMpxClient btMpxClient() {
        return new GsonBtMpxClient(httpClient(), baseUri);
    }
    
    private SimpleHttpClient httpClient() {
        return new SimpleHttpClientBuilder().build();
    }
    
    @Bean 
    public BtChannelGroupUpdater productionChannelGroupUpdater() {
        return perEnvironmentChannelGroupUpdater(Publisher.BT_TV_CHANNELS, ALIAS_NAMESPACE_PREFIX, baseUri);
    }
    
    @Bean 
    public BtChannelGroupUpdater dev1ChannelGroupUpdater() {
        return perEnvironmentChannelGroupUpdater(Publisher.BT_TV_CHANNELS_TEST1, ALIAS_NAMESPACE_PREFIX, test1BaseUri);
    }
    
    @Bean 
    public BtChannelGroupUpdater dev2ChannelGroupUpdater() {
        return perEnvironmentChannelGroupUpdater(Publisher.BT_TV_CHANNELS_TEST2, ALIAS_NAMESPACE_PREFIX, test2BaseUri);
    }
    
    @Bean 
    public BtChannelGroupUpdater dev3ChannelGroupUpdater() {
        return perEnvironmentChannelGroupUpdater(Publisher.BT_TV_CHANNELS_REFERENCE, ALIAS_NAMESPACE_PREFIX, test3BaseUri);
    }
    
    private BtChannelGroupUpdater perEnvironmentChannelGroupUpdater(Publisher publisher, 
            String aliasNamespacePrefix, String mpxUriBase) {
        GsonBtMpxClient mpxClient = new GsonBtMpxClient(httpClient(), baseUri);
        
        BtAllChannelsChannelGroupUpdater btAllChannelsChannelGroupUpdater 
            = new BtAllChannelsChannelGroupUpdater(channelGroupWriter, 
                channelGroupResolver, freeviewPlatformChannelGroupId, 
                uriPrefixFromPublisher(publisher), publisher);
        
        return new BtChannelGroupUpdater(mpxClient, publisher, uriPrefixFromPublisher(publisher), 
                aliasNamespacePrefix, channelGroupResolver, channelGroupWriter, 
                channelResolver, channelWriter, btAllChannelsChannelGroupUpdater, channelWriterLock);
        
    }
    
    public String uriPrefixFromPublisher(Publisher publisher) {
        return String.format(URI_PREFIX_STRING_FORMAT, publisher.key());
    }
    
    @PostConstruct
    public void scheduleTasks() {
        scheduler.schedule(productionChannelGroupUpdater()
                .withName("BT Channel Group (PROD) Ingester"), 
                PROD_INGEST_REPETITION);
        
        scheduler.schedule(dev1ChannelGroupUpdater()
                .withName("BT Channel Group (TEST1) Ingester"), 
                TEST1_INGEST_REPETITION);
        
        scheduler.schedule(dev2ChannelGroupUpdater()
                .withName("BT Channel Group (TEST2) Ingester"), 
                TEST2_INGEST_REPETITION);
        
        scheduler.schedule(dev3ChannelGroupUpdater()
                .withName("BT Channel Group (REFERENCE) Ingester"), 
                REFERENCE_INGEST_REPETITION);
    }
    
}
