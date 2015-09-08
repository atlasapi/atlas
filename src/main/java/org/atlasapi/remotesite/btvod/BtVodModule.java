package org.atlasapi.remotesite.btvod;

import java.util.Map;

import javax.annotation.PostConstruct;

import com.metabroadcast.common.scheduling.ScheduledTask;
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
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class BtVodModule {

    private static final String BT_VOD_NAMESPACES_PREFIX = "gb:bt:tv:mpx:";
    private static final String BT_VOD_FEED_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "feed:%s:%s";
    private static final String BT_VOD_APP_CATEGORY_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "category:%s:%s";
    private static final String BT_VOD_CONTENT_PROVIDER_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "contentProvider:%s:%s";
    private static final String BT_VOD_GENRE_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "genre:%s:%s";
    private static final String BT_VOD_GUID_ALIAS_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "guid:%s:%s";
    private static final String BT_VOD_ID_ALIAS_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "id:%s:%s";
    private static final String BT_VOD_NEW_FEED = "new";
    private static final String BT_VOD_KIDS_TOPIC = "kids";
    private static final String BT_VOD_CATCHUP_TOPIC = "subscription-catchup";
    private static final String BT_VOD_TV_BOXSETS_TOPIC = "tv-box-sets";
    
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
    private static final String TVE_URI_PREFIX_FORMAT = "http://%s/";
    private static final ImmutableSet<String> SEASON_PRODUCT_OFFERING_TYPES = ImmutableSet.of("season", "season-est");
    private static final String SUBSCRIPTION_CATCHUP_SCHEDULER_CHANNEL = "TV Replay";
    
    private static final RepetitionRule TVE_MPX_REPETITION_RULE = RepetitionRules.daily(new LocalTime(7, 0, 0));
    private static final RepetitionRule MPX_REPETITION_RULE = RepetitionRules.daily(new LocalTime(11, 0, 0));

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

    @Value("${bt.vod.mpx.prod.feed.baseUrl}")
    private String btVodMpxProdFeedBaseUrl;
    @Value("${bt.vod.mpx.prod.feed.name}")
    private String btVodMpxProdFeedName;
    @Value("${bt.vod.mpx.prod.feed.params.q}")
    private String btVodMpxProdFeedQParam;

    @Value("${bt.vod.mpx.vold.feed.baseUrl}")
    private String btVodMpxVoldFeedBaseUrl;
    @Value("${bt.vod.mpx.vold.feed.name}")
    private String btVodMpxVoldFeedName;
    @Value("${bt.vod.mpx.vold.feed.params.q}")
    private String btVodMpxVoldFeedQParam;

    @Value("${bt.vod.mpx.vole.feed.baseUrl}")
    private String btVodMpxVoleFeedBaseUrl;
    @Value("${bt.vod.mpx.vole.feed.name}")
    private String btVodMpxVoleFeedName;
    @Value("${bt.vod.mpx.vole.feed.params.q}")
    private String btVodMpxVoleFeedQParam;

    @Value("${bt.vod.mpx.systest2.feed.baseUrl}")
    private String btVodMpxSystest2FeedBaseUrl;
    @Value("${bt.vod.mpx.systest2.feed.name}")
    private String btVodMpxSystest2FeedName;
    @Value("${bt.vod.mpx.systest2.feed.params.q}")
    private String btVodMpxSystest2FeedQParam;


    private BtVodStaleTopicContentRemover staleTopicContentRemover(Publisher publisher, String env, String conf) {
        return new BtVodStaleTopicContentRemover(
                ImmutableSet.of(
                        topicFor(feedNamepaceFor(env, conf), BT_VOD_NEW_FEED, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_TV_BOXSETS_TOPIC, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_KIDS_TOPIC, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_CATCHUP_TOPIC, publisher)
                ),
                topicContentLister,
                contentWriter
        );
    }

    private BtVodUpdater btVodUpdater() {
        return new BtVodUpdater(
                mergingContentWriter(),
                btVodData(btVodMpxProdFeedBaseUrl, btVodMpxProdFeedName, btVodMpxProdFeedQParam),
                URI_PREFIX,
                btVodContentGroupUpdater(Publisher.BT_VOD, URI_PREFIX, btVodMpxProdFeedBaseUrl, btVodMpxProdFeedQParam),
                Publisher.BT_VOD,
                oldContentDeactivator(Publisher.BT_VOD),
                noImageExtractor(),
                URI_PREFIX,
                noImageExtractor(),
                brandUriExtractor(URI_PREFIX),
                topicResolver,
                topicWriter,
                newFeedContentMatchingPredicate(btVodMpxProdFeedBaseUrl, btVodMpxProdFeedQParam),
                topicFor(feedNamepaceFor("prod", "config1"), BT_VOD_NEW_FEED, Publisher.BT_VOD),
                topicFor(btVodAppCategoryNamespaceFor("prod", "config1"), BT_VOD_KIDS_TOPIC, Publisher.BT_VOD),
                topicFor(btVodAppCategoryNamespaceFor("prod", "config1"), BT_VOD_TV_BOXSETS_TOPIC, Publisher.BT_VOD),
                topicFor(btVodAppCategoryNamespaceFor("prod", "config1"), BT_VOD_CATCHUP_TOPIC, Publisher.BT_VOD),
                staleTopicContentRemover(Publisher.BT_VOD, "prod", "config1"),
                seriesUriExtractor(URI_PREFIX),
                versionsExtractor(URI_PREFIX),
                describedFieldsExtractor(Publisher.BT_VOD,"prod", "config1", btVodMpxProdFeedBaseUrl, btVodMpxProdFeedQParam)
        );
    }

    @Bean
    public ScheduledTask btTveVodProdConfig1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD,
                "prod",
                "config1",
                btVodMpxProdFeedBaseUrl,
                btVodMpxProdFeedName,
                btVodMpxProdFeedQParam
        );
    }

    @Bean
    public ScheduledTask btTveVodVoleConfig1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD_VOLE_CONFIG_1,
                "vole",
                "config1",
                btVodMpxVoleFeedBaseUrl,
                btVodMpxVoleFeedName,
                btVodMpxVoleFeedQParam
        );
    }

    @Bean
    public ScheduledTask btTveVodVoldConfig1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD_VOLD_CONFIG_1,
                "vold",
                "config1",
                btVodMpxVoldFeedBaseUrl,
                btVodMpxVoldFeedName,
                btVodMpxVoldFeedQParam
        );
    }

    @Bean
    public ScheduledTask btTveVodSystest2Config1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD_SYSTEST2_CONFIG_1,
                "systest2",
                "config1",
                btVodMpxSystest2FeedBaseUrl,
                btVodMpxSystest2FeedName,
                btVodMpxSystest2FeedQParam
        );
    }

    private ScheduledTask btVodUpdater(
            Publisher publisher,
            String envName,
            String conf,
            String feedBaseUrl,
            String feedName,
            String feedQParam
    ) {
        String uriPrefix = String.format(TVE_URI_PREFIX_FORMAT, publisher.key());
        return new BtVodUpdater(
                mergingContentWriter(),
                btVodData(feedBaseUrl, feedName, feedQParam),
                uriPrefix,
                btVodContentGroupUpdater(publisher, uriPrefix, feedBaseUrl, feedQParam),
                publisher,
                oldContentDeactivator(publisher),
                brandImageExtractor(btPortalBaseUri),
                uriPrefix,
                itemImageExtractor(),
                brandUriExtractor(uriPrefix),
                topicResolver,
                topicWriter,
                newFeedContentMatchingPredicate(feedBaseUrl, feedQParam),
                topicFor(feedNamepaceFor(envName, conf), BT_VOD_NEW_FEED, publisher),
                topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_KIDS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_TV_BOXSETS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_CATCHUP_TOPIC, publisher),
                staleTopicContentRemover(Publisher.BT_TVE_VOD, envName, conf), seriesUriExtractor(uriPrefix),
                versionsExtractor(uriPrefix),
                describedFieldsExtractor(
                        publisher,
                        envName,
                        conf,
                        feedBaseUrl,
                        feedQParam
                )
        ).withName(
                String.format(
                        "BT TVE VoD Updater for %s",
                        publisher.key()
                )
        );
    }


    private String feedNamepaceFor(String env, String conf) {
        return String.format(BT_VOD_FEED_NAMESPACE_FORMAT, env, conf);
    }

    private String btVodAppCategoryNamespaceFor(String env, String conf) {
           return String.format(BT_VOD_APP_CATEGORY_NAMESPACE_FORMAT, env, conf);
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
    
    public BtVodDescribedFieldsExtractor describedFieldsExtractor(
            Publisher publisher,
            String env,
            String conf,
            String baseUrl,
            String qParam
    ) {
        BtVodContentMatchingPredicate newContentPredicate = newFeedContentMatchingPredicate(baseUrl, qParam);
        newContentPredicate.init();
        return new BtVodDescribedFieldsExtractor(
                topicResolver,
                topicWriter,
                publisher,
                newContentPredicate,
                contentGroupsAndCriteria(baseUrl, qParam).get(KIDS_CATEGORY.toLowerCase()),
                BtVodContentMatchingPredicates.schedulerChannelAndOfferingTypePredicate(
                        TV_CATEGORY, SEASON_PRODUCT_OFFERING_TYPES
                ),
                BtVodContentMatchingPredicates.schedulerChannelPredicate(SUBSCRIPTION_CATCHUP_SCHEDULER_CHANNEL),
                topicFor(feedNamepaceFor(env, conf), BT_VOD_NEW_FEED, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_KIDS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_TV_BOXSETS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_CATCHUP_TOPIC, publisher),
                String.format(BT_VOD_GUID_ALIAS_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_ID_ALIAS_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_CONTENT_PROVIDER_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_GENRE_NAMESPACE_FORMAT, env, conf)
        );
    }
    
    public DerivingFromSeriesBrandImageExtractor brandImageExtractor(String baseUrl) {
        return new DerivingFromSeriesBrandImageExtractor(brandUriExtractor(baseUrl), seriesUriExtractor(baseUrl), itemImageExtractor());
    }
    
    public ImageExtractor itemImageExtractor() {
        return new BtVodMpxImageExtractor(btPortalBaseUri);
    }
    
    public BrandUriExtractor brandUriExtractor(String uriPrefix) {
        return new BrandUriExtractor(uriPrefix, titleSanitiser());
    }
    
    private TitleSanitiser titleSanitiser() {
        return new TitleSanitiser();
    }
    public NoImageExtractor noImageExtractor() {
        return new NoImageExtractor();
    }
    
    public BtVodContentGroupUpdater btVodContentGroupUpdater(Publisher publisher, String uriPrefix, String baseUrl, String qParam) {
        return new BtVodContentGroupUpdater(contentGroupResolver, contentGroupWriter, 
                contentGroupsAndCriteria(baseUrl, qParam), uriPrefix, publisher);
    }
    
    private BtVodData btVodData(String baseUrl, String feedName,   String qParam) {
        return new BtVodData(
                mpxVodClient(baseUrl, qParam),
                feedName
        );
    }

    private HttpBtMpxVodClient mpxVodClient(String baseUrl, String qParam) {
        return new HttpBtMpxVodClient(
                new SimpleHttpClientBuilder().withUserAgent(HttpClients.ATLAS_USER_AGENT).build(),
                new HttpBtMpxFeedRequestProvider(baseUrl, qParam)
        );
    }
    
    private Map<String, BtVodContentMatchingPredicate> contentGroupsAndCriteria(String baseUrl, String qParam) {
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
                .put(NEW_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(baseUrl, qParam), NEW_CONTENT_MPX_FEED_NAME))
                .build();
    }
    
    private Topic topicFor(String namespace, String topicName, Publisher publisher) {
        Topic topic = topicResolver.topicFor(publisher, namespace, topicName).requireValue();
        topicWriter.write(topic);
        return topic;
    }
    
    private BtVodContentMatchingPredicate newFeedContentMatchingPredicate(String baseUrl, String qParam) {
        return BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(baseUrl, qParam), NEW_CONTENT_MPX_FEED_NAME);
    }

    private MergingContentWriter mergingContentWriter() {
        return new MergingContentWriter(contentWriter,contentResolver);
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
        scheduler.schedule(btVodUpdater().withName("BT VoD Updater"), MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodProdConfig1Updater(), TVE_MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodSystest2Config1Updater(), TVE_MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodVoldConfig1Updater(), TVE_MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodVoleConfig1Updater(), TVE_MPX_REPETITION_RULE);
    }
}
