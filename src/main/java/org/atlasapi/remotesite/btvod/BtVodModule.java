package org.atlasapi.remotesite.btvod;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupPredicates;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.atlasapi.remotesite.btvod.portal.PortalClient;
import org.atlasapi.remotesite.btvod.portal.XmlPortalClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class BtVodModule {

    private static final String PORTAL_BOXSET_GROUP = "03_tv/40_searcha-z/all";
    private static final String PORTAL_BOXOFFICE_GROUP = "01_boxoffice/05_new/all";
    private static final String PORTAL_BUY_TO_OWN_GROUP = "01_boxoffice/Must_Own_Movies_Categories/New_To_Own";
    private static final String BOX_OFFICE_PICKS_GROUP = "50_misc_car_you/Misc_metabroadcast/Misc_metabroadcast_1";
    
    private static final String NEW_CONTENT_MPX_FEED_NAME = "btv-prd-nav-new";
    
    private static final String MUSIC_CATEGORY = "Music";
    private static final String FILM_CATEGORY = "Film";
    private static final String TV_CATEGORY = "TV";
    private static final String KIDS_CATEGORY = "Kids";
    private static final String SPORT_CATEGORY = "Sport";
    private static final String BUY_TO_OWN_CATEGORY = "BuyToOwn";
    private static final String TV_BOX_SETS_CATEGORY = "TvBoxSets";
    private static final String BOX_OFFICE_CATEGORY = "BoxOffice";
    private static final String CZN_CONTENT_PROVIDER_ID = "CHC";
    private static final String BOX_OFFICE_PICKS_CATEGORY = "BoxOfficePicks";
    private static final String NEW_CATEGORY = "New";
    
    private static final String URI_PREFIX = "http://vod.bt.com/";
    
    @Autowired
    private SimpleScheduler scheduler;
    @Autowired
    private ContentResolver contentResolver;
    @Autowired
    private ContentWriter contentWriter;
    @Autowired
    private ContentGroupResolver contentGroupResolver;
    @Autowired
    private ContentGroupWriter contentGroupWriter;
    @Value("${bt.vod.file}")
    private String filename;
    @Value("${bt.portal.baseUri}")
    private String btPortalBaseUri;
    @Value("${bt.portal.contentGroups.baseUri}")
    private String btPortalContentGroupsBaseUri;
    @Value("${bt.vod.mpx.feed.baseUrl}")
    private String btVodMpxFeedBaseUrl;
    @Value("${bt.vod.mpx.feed.params.q}")
    private String btVodMpxFeedQParam;

    @Bean
    public BtVodUpdater btVodUpdater() {
        return new BtVodUpdater(contentResolver, 
                contentWriter, btVodData(), URI_PREFIX, btVodContentGroupUpdater(), 
                describedFieldsExtractor(), Publisher.BT_VOD);
    }
    
    @Bean
    public BtVodDescribedFieldsExtractor describedFieldsExtractor() {
        return new BtVodDescribedFieldsExtractor(new BtVodMpxImageExtractor(btPortalBaseUri));
    }
    
    @Bean
    public BtVodContentGroupUpdater btVodContentGroupUpdater() {
        return new BtVodContentGroupUpdater(contentGroupResolver, contentGroupWriter, 
                contentGroupsAndCriteria(), URI_PREFIX, Publisher.BT_VOD);
    }
    
    private BtVodData btVodData() {
        return new BtVodData(
                mpxVodClient()
        );
    }

    @Bean
    public HttpBtMpxVodClient mpxVodClient() {
        return new HttpBtMpxVodClient(
                new SimpleHttpClientBuilder().withUserAgent(HttpClients.ATLAS_USER_AGENT).build(),
                new HttpBtMpxFeedRequestProvider(btVodMpxFeedBaseUrl, btVodMpxFeedQParam)
        );
    }
    
    private Map<String, BtVodContentGroupPredicate> contentGroupsAndCriteria() {
        return ImmutableMap.<String, BtVodContentGroupPredicate> builder()
                .put(MUSIC_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.schedulerChannelPredicate(MUSIC_CATEGORY))
                .put(FILM_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.filmPredicate())
                .put(TV_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.schedulerChannelPredicate(TV_CATEGORY))
                .put(KIDS_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.schedulerChannelPredicate(KIDS_CATEGORY))
                .put(SPORT_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.schedulerChannelPredicate(SPORT_CATEGORY))
                .put(CZN_CONTENT_PROVIDER_ID.toLowerCase(), BtVodContentGroupPredicates.cznPredicate())
                .put(BUY_TO_OWN_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.portalContentGroupPredicate(portalClient(), PORTAL_BUY_TO_OWN_GROUP, null))
                .put(BOX_OFFICE_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.portalContentGroupPredicate(portalClient(), PORTAL_BOXOFFICE_GROUP, null))
                .put(TV_BOX_SETS_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.portalContentGroupPredicate(portalClient(), PORTAL_BOXSET_GROUP, Series.class))
                .put(BOX_OFFICE_PICKS_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.portalContentGroupPredicate(portalClient(), BOX_OFFICE_PICKS_GROUP, null))
                .put(NEW_CATEGORY.toLowerCase(), BtVodContentGroupPredicates.mpxContentGroupPredicate(mpxVodClient(), NEW_CONTENT_MPX_FEED_NAME))
                .build();
    }
    
    @Bean
    public PortalClient portalClient() {
        return new XmlPortalClient(btPortalContentGroupsBaseUri, 
                new SimpleHttpClientBuilder()
                        .withUserAgent(HttpClients.ATLAS_USER_AGENT)
                        .withRetries(3)
                        .build());
    }
    
    @PostConstruct
    public void scheduleTask() {
        scheduler.schedule(btVodUpdater(), RepetitionRules.NEVER);
    }
}
