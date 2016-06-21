package org.atlasapi.remotesite.thesun;

import javax.annotation.PostConstruct;

import nu.xom.Builder;
import nu.xom.Document;

import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.persistence.system.RemoteSiteClient;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.channel4.pmlsd.XmlClient;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.http.RequestLimitingSimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;

@Configuration
public class TheSunModule {
    private final static Daily ONCE_DAILY = RepetitionRules.daily(new LocalTime(2, 0, 0));
    private @Autowired SimpleScheduler scheduler;
    private @Autowired @Qualifier("contentResolver") ContentResolver contentResolver;
    private @Autowired @Qualifier("contentWriter") ContentWriter contentWriter;
    private @Autowired ContentGroupWriter contentGroupWriter;
    private @Autowired ContentGroupResolver contentGroupResolver;
    private @Value("${thesuntvpicks.rss.url}") String feedUrl;
    private @Value("${thesuntvpicks.contentgroup.uri}") String contentGroupUri;
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(theSunTvPicksUpdater() , ONCE_DAILY);
    }
    
    @Bean 
    public RemoteSiteClient<Document> theSunTvPicksClient() {
        return new XmlClient(requestLimitedHttpClient(), new Builder(new TheSunTvPicksElementFactory()));
    }
    
    @Bean 
    protected SimpleHttpClient requestLimitedHttpClient() {
        return new RequestLimitingSimpleHttpClient(HttpClients.webserviceClient(), 4);
    }
    
    @Bean
    public TheSunTvPicksUpdater theSunTvPicksUpdater() {
        TheSunTvPicksEntryProcessor entryProcessor = new TheSunTvPicksEntryProcessor(contentWriter, contentResolver);
        TheSunTvPicksContentGroupUpdater groupUpdater = new TheSunTvPicksContentGroupUpdater(contentGroupResolver, contentGroupWriter);
        return new TheSunTvPicksUpdater(feedUrl, contentGroupUri, theSunTvPicksClient(), entryProcessor, groupUpdater);
    }
}
