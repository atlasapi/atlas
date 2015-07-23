package org.atlasapi.remotesite.btvod;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.atlasapi.remotesite.btvod.portal.PortalClient;
import org.atlasapi.remotesite.btvod.portal.XmlPortalClient;
import org.atlasapi.remotesite.btvod.topics.BtVodStaleTopicContentRemover;
import org.atlasapi.remotesite.util.OldContentDeactivator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class BtVodModule {

    static final String BT_VOD_FEED_NAMESPACE = "gb:bt:tv:mpx:prod:feed";
    static final String BT_VOD_NEW_FEED = "new";
    
    private static final int THRESHOLD_FOR_NOT_REMOVING_OLD_CONTENT = 25;
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
    private static final String TVE_URI_PREFIX = "http://tve-vod.bt.com/";
    
    @Autowired
    private SimpleScheduler scheduler;
    @Autowired
    private ContentResolver contentResolver;
    @Autowired
    private ContentWriter contentWriter;
    @Autowired
    private ContentLister contentLister;
    @Autowired
    private ContentGroupResolver contentGroupResolver;
    @Autowired
    private ContentGroupWriter contentGroupWriter;
    @Autowired
    private TopicCreatingTopicResolver topicResolver;
    @Autowired
    private TopicContentLister topicContentLister;
    @Autowired
    @Qualifier("topicStore")
    private TopicWriter topicWriter;
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
        return new BtVodUpdater(
                contentResolver,
                contentWriter,
                btVodData(),
                URI_PREFIX,
                btVodContentGroupUpdater(Publisher.BT_VOD, URI_PREFIX),
                Publisher.BT_VOD,
                oldContentDeactivator(Publisher.BT_VOD),
                noImageExtractor(),
                URI_PREFIX,
                noImageExtractor(),
                brandUriExtractor(URI_PREFIX),
                topicResolver,
                topicWriter,
                newFeedContentMatchingPredicate(),
                newTopic(Publisher.BT_VOD),
                staleTopicContentRemover(Publisher.BT_VOD),
                seriesUriExtractor(URI_PREFIX),
                versionsExtractor(URI_PREFIX)
        );
    }
    
    private BtVodStaleTopicContentRemover staleTopicContentRemover(Publisher publisher) {
        return new BtVodStaleTopicContentRemover(ImmutableSet.of(newTopic(publisher)), topicContentLister, contentWriter);
    }

    @Bean
    public BtVodUpdater btTveVodUpdater() {
        return new BtVodUpdater(
                contentResolver,
                contentWriter,
                btVodData(),
                TVE_URI_PREFIX,
                btVodContentGroupUpdater(Publisher.BT_TVE_VOD, TVE_URI_PREFIX),
                Publisher.BT_TVE_VOD,
                oldContentDeactivator(Publisher.BT_TVE_VOD),
                brandImageExtractor(TVE_URI_PREFIX),
                TVE_URI_PREFIX,
                itemImageExtractor(),
                brandUriExtractor(TVE_URI_PREFIX),
                topicResolver,
                topicWriter,
                newFeedContentMatchingPredicate(),
                newTopic(Publisher.BT_TVE_VOD),
                staleTopicContentRemover(Publisher.BT_TVE_VOD), seriesUriExtractor(TVE_URI_PREFIX),
                versionsExtractor(TVE_URI_PREFIX)
        );
    }
    
    private BtVodOldContentDeactivator oldContentDeactivator(Publisher publisher) {
        return new BtVodOldContentDeactivator(
                        publisher, 
                        new OldContentDeactivator(contentLister, contentWriter, contentResolver), 
                        THRESHOLD_FOR_NOT_REMOVING_OLD_CONTENT);
    }

    private BtVodVersionsExtractor versionsExtractor(String prefix) {
        return new BtVodVersionsExtractor(new BtVodPricingAvailabilityGrouper(), prefix);
    }

    private BtVodSeriesUriExtractor seriesUriExtractor(String prefix) {
        return new BtVodSeriesUriExtractor(brandUriExtractor(prefix));
    }
    
    public BtVodDescribedFieldsExtractor describedFieldsExtractor(Publisher publisher) {
        return new BtVodDescribedFieldsExtractor(topicResolver, topicWriter, publisher, newFeedContentMatchingPredicate(), newTopic(publisher));
    }
    
    public DerivingFromSeriesBrandImageExtractor brandImageExtractor(String baseUrl) {
        return new DerivingFromSeriesBrandImageExtractor(brandUriExtractor(baseUrl), baseUrl, seriesUriExtractor(baseUrl));
    }
    
    public ImageExtractor itemImageExtractor() {
        return new BtVodMpxImageExtractor(btPortalBaseUri);
    }
    
    public BrandUriExtractor brandUriExtractor(String uriPrefix) {
        return new BrandUriExtractor(uriPrefix, titleSanitiser());
    }
    
    @Bean
    public TitleSanitiser titleSanitiser() {
        return new TitleSanitiser();
    }
    public NoImageExtractor noImageExtractor() {
        return new NoImageExtractor();
    }
    
    public BtVodContentGroupUpdater btVodContentGroupUpdater(Publisher publisher, String uriPrefix) {
        return new BtVodContentGroupUpdater(contentGroupResolver, contentGroupWriter, 
                contentGroupsAndCriteria(), uriPrefix, publisher);
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
    
    private Map<String, BtVodContentMatchingPredicate> contentGroupsAndCriteria() {
        return ImmutableMap.<String, BtVodContentMatchingPredicate> builder()
                .put(MUSIC_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.schedulerChannelPredicate(MUSIC_CATEGORY))
                .put(FILM_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.filmPredicate())
                .put(TV_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.schedulerChannelPredicate(TV_CATEGORY))
                .put(KIDS_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.schedulerChannelPredicate(KIDS_CATEGORY))
                .put(SPORT_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.schedulerChannelPredicate(SPORT_CATEGORY))
                .put(CZN_CONTENT_PROVIDER_ID.toLowerCase(), BtVodContentMatchingPredicates.cznPredicate())
                .put(BUY_TO_OWN_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.portalGroupContentMatchingPredicate(portalClient(), PORTAL_BUY_TO_OWN_GROUP, null))
                .put(BOX_OFFICE_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.portalGroupContentMatchingPredicate(portalClient(), PORTAL_BOXOFFICE_GROUP, null))
                .put(TV_BOX_SETS_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.portalGroupContentMatchingPredicate(portalClient(), PORTAL_BOXSET_GROUP, Series.class))
                .put(BOX_OFFICE_PICKS_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.portalGroupContentMatchingPredicate(portalClient(), BOX_OFFICE_PICKS_GROUP, null))
                .put(NEW_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(), NEW_CONTENT_MPX_FEED_NAME))
                .build();
    }
    
    private Topic newTopic(Publisher publisher) {
        Topic topic = topicResolver.topicFor(publisher, BT_VOD_FEED_NAMESPACE, BT_VOD_NEW_FEED).requireValue();
        topicWriter.write(topic);
        return topic;
    }
    
    private BtVodContentMatchingPredicate newFeedContentMatchingPredicate() {
        return BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(), NEW_CONTENT_MPX_FEED_NAME);
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
        scheduler.schedule(btVodUpdater().withName("BT VoD Updater"), RepetitionRules.NEVER);
        scheduler.schedule(btTveVodUpdater().withName("BT TVE VoD Updater"), RepetitionRules.NEVER);
    }
}
