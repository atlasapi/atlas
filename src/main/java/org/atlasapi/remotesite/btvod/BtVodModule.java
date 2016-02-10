package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentMatchingPredicates;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodEntryMatchingPredicates;
import org.atlasapi.remotesite.btvod.portal.PortalClient;
import org.atlasapi.remotesite.btvod.portal.XmlPortalClient;
import org.atlasapi.remotesite.util.OldContentDeactivator;

import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BtVodModule {
    private static final String BT_VOD_UPDATER_ENV = "prod";
    private static final String BT_VOD_UPDATER_CONFIG = "config1";

    private static final String BT_VOD_NAMESPACES_PREFIX = "gb:bt:tv:mpx:";
    private static final String BT_VOD_FEED_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "feed:%s:%s";
    private static final String BT_VOD_APP_CATEGORY_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "category:%s:%s";
    private static final String BT_VOD_CONTENT_PROVIDER_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "contentProvider:%s:%s";
    private static final String BT_VOD_GENRE_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "genre:%s:%s";
    private static final String BT_VOD_KEYWORD_NAMESPACE_FORMAT = BT_VOD_NAMESPACES_PREFIX + "keyword:%s:%s";
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

    private static final RepetitionRule TVE_MPX_REPETITION_RULE = RepetitionRules.every(Duration.standardHours(6));

    // These times were selected to avoid conflicts with MPX maintenance at
    // 02:22 / 05:22 / 08:22 / 11:22 / 14:22 / 17:22 / 20:22 / 23:22
    private static final ImmutableList<LocalTime> TVE_MPX_DAILY_INGEST_TIMES = ImmutableList.of(
            new LocalTime(0, 0),
            new LocalTime(3, 0),
            new LocalTime(6, 0),
            new LocalTime(9, 0),
            new LocalTime(12, 0),
            new LocalTime(15, 0),
            new LocalTime(18, 0),
            new LocalTime(21, 0)
    );

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
    private TopicQueryResolver topicQueryResolver;
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
    @Value("${bt.vod.mpx.prod.feed.guidLookup.baseUrl}")
    private String btVodMpxProdFeedBaseUrlForGuidLookup;
    @Value("${bt.vod.mpx.prod.feed.guidLookup.name}")
    private String btVodMpxProdFeedNameForGuidLookup;
    
    @Value("${bt.vod.mpx.prod.feed.params.q}")
    private String btVodMpxProdFeedQParam;
    @Value("${bt.vod.mpx.prod.feed.new.suffix}")
    private String btVodMpxProdFeedNewSuffix;

    @Value("${bt.vod.mpx.vold.feed.baseUrl}")
    private String btVodMpxVolDFeedBaseUrl;
    @Value("${bt.vod.mpx.vold.feed.name}")
    private String btVodMpxVolDFeedName;
    @Value("${bt.vod.mpx.vold.feed.params.q}")
    private String btVodMpxVolDFeedQParam;
    @Value("${bt.vod.mpx.vold.feed.new.suffix}")
    private String btVodMpxVolDFeedNewSuffix;

    @Value("${bt.vod.mpx.vole.feed.baseUrl}")
    private String btVodMpxVoleFeedBaseUrl;
    @Value("${bt.vod.mpx.vole.feed.name}")
    private String btVodMpxVoleFeedName;
    @Value("${bt.vod.mpx.vole.feed.params.q}")
    private String btVodMpxVoleFeedQParam;
    @Value("${bt.vod.mpx.vole.feed.new.suffix}")
    private String btVodMpxVolEFeedNewSuffix;

    @Value("${bt.vod.mpx.systest2.feed.baseUrl}")
    private String btVodMpxSystest2FeedBaseUrl;
    @Value("${bt.vod.mpx.systest2.feed.name}")
    private String btVodMpxSystest2FeedName;
    @Value("${bt.vod.mpx.systest2.feed.params.q}")
    private String btVodMpxSystest2FeedQParam;
    @Value("${bt.vod.mpx.systest2.feed.new.suffix}")
    private String btVodMpxSystest2FeedNewSuffix;

    @Value("${service.bttv.id}")
    private Long btTvServiceId;
    
    @Value("${service.bttvotg.id}")
    private Long btTvOtgServiceId;

    private BtVodUpdater btVodUpdater(String newFeedSuffix, Map<String, BtVodContentMatchingPredicate> contentGroupsAndCritera) {
        return new BtVodUpdater(
                contentWriter,
                btVodData(btVodMpxProdFeedBaseUrl, btVodMpxProdFeedName, btVodMpxProdFeedQParam),
                URI_PREFIX,
                btVodContentGroupUpdater(Publisher.BT_VOD, URI_PREFIX, btVodMpxProdFeedBaseUrl, btVodMpxProdFeedQParam, contentGroupsAndCritera),
                Publisher.BT_VOD,
                oldContentDeactivator(Publisher.BT_VOD),
                noImageExtractor(),
                brandUriExtractor(URI_PREFIX),
                newFeedContentMatchingPredicate(btVodMpxProdFeedBaseUrl, newFeedSuffix, btVodMpxProdFeedQParam),
                ImmutableSet.of(
                        topicFor(feedNamepaceFor(BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG), BT_VOD_NEW_FEED, Publisher.BT_VOD),
                        topicFor(btVodAppCategoryNamespaceFor(BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG), BT_VOD_KIDS_TOPIC, Publisher.BT_VOD),
                        topicFor(btVodAppCategoryNamespaceFor(BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG), BT_VOD_TV_BOXSETS_TOPIC, Publisher.BT_VOD),
                        topicFor(btVodAppCategoryNamespaceFor(BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG), BT_VOD_CATCHUP_TOPIC, Publisher.BT_VOD)
                ),
                ImmutableSet.of(String.format(BT_VOD_KEYWORD_NAMESPACE_FORMAT, BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG)),
                seriesUriExtractor(URI_PREFIX),
                versionsExtractor(URI_PREFIX, BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG),
                describedFieldsExtractor(Publisher.BT_VOD, BT_VOD_UPDATER_ENV, BT_VOD_UPDATER_CONFIG,
                        btVodMpxProdFeedBaseUrl, newFeedSuffix, btVodMpxProdFeedQParam, btVodMpxProdFeedBaseUrl),
                mpxVodClient(btVodMpxProdFeedBaseUrl, btVodMpxProdFeedName, btVodMpxProdFeedQParam),
                topicQueryResolver,
                BtVodEntryMatchingPredicates.schedulerChannelPredicate(KIDS_CATEGORY)
        );
    }

    private ScheduledTask btTveVodProdConfig1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD,
                "prod",
                "config1",
                btVodMpxProdFeedBaseUrl,
                btVodMpxProdFeedName,
                btVodMpxProdFeedQParam,
                btVodMpxProdFeedNewSuffix, 
                btVodMpxProdFeedBaseUrlForGuidLookup,
                btVodMpxProdFeedNameForGuidLookup,
                ImmutableMap.<String, BtVodContentMatchingPredicate>of()
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
                btVodMpxVoleFeedQParam,
                btVodMpxVolEFeedNewSuffix,
                btVodMpxVoleFeedBaseUrl,
                btVodMpxVoleFeedName,
                ImmutableMap.<String, BtVodContentMatchingPredicate>of()
        );
    }

    @Bean
    public ScheduledTask btTveVodVoldConfig1Updater() {
        return btVodUpdater(
                Publisher.BT_TVE_VOD_VOLD_CONFIG_1,
                "vold",
                "config1",
                btVodMpxVolDFeedBaseUrl,
                btVodMpxVolDFeedName,
                btVodMpxVolDFeedQParam,
                btVodMpxVolDFeedNewSuffix,
                btVodMpxVolDFeedBaseUrl,
                btVodMpxVolDFeedName,
                ImmutableMap.<String, BtVodContentMatchingPredicate>of()
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
                btVodMpxSystest2FeedQParam,
                btVodMpxSystest2FeedNewSuffix,
                btVodMpxSystest2FeedBaseUrl,
                btVodMpxSystest2FeedName,
                ImmutableMap.<String, BtVodContentMatchingPredicate>of()
        );
    }

    private ScheduledTask btVodUpdater(
            Publisher publisher,
            String envName,
            String conf,
            String feedBaseUrl,
            String feedName,
            String feedQParam,
            String newFeedSuffix,
            String baseUrlForItemLookup,
            String feedNameForItemLookup,
            Map<String, BtVodContentMatchingPredicate> contentGroupsAndCritera
    ) {
        String uriPrefix = String.format(TVE_URI_PREFIX_FORMAT, publisher.key());
        return new BtVodUpdater(
                contentWriter,
                btVodData(feedBaseUrl, feedName, feedQParam),
                uriPrefix,
                btVodContentGroupUpdater(publisher, uriPrefix, feedBaseUrl, feedQParam, contentGroupsAndCritera),
                publisher,
                oldContentDeactivator(publisher),
                itemImageExtractor(),
                brandUriExtractor(uriPrefix),
                newFeedContentMatchingPredicate(baseUrlForItemLookup, feedNameForItemLookup, feedQParam),
                ImmutableSet.of(
                        topicFor(feedNamepaceFor(envName, conf), BT_VOD_NEW_FEED, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_KIDS_TOPIC, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_TV_BOXSETS_TOPIC, publisher),
                        topicFor(btVodAppCategoryNamespaceFor(envName, conf), BT_VOD_CATCHUP_TOPIC, publisher)
                ),
                ImmutableSet.of(String.format(BT_VOD_KEYWORD_NAMESPACE_FORMAT, envName, conf),
                                String.format(BT_VOD_CONTENT_PROVIDER_NAMESPACE_FORMAT, envName, conf)
                               ),
                seriesUriExtractor(uriPrefix),
                versionsExtractor(uriPrefix, envName, conf),
                describedFieldsExtractor(
                        publisher,
                        envName,
                        conf,
                        feedBaseUrl,
                        newFeedSuffix,
                        feedQParam,
                        baseUrlForItemLookup
                ),
                mpxVodClient(baseUrlForItemLookup, feedNameForItemLookup, btVodMpxProdFeedQParam),
                topicQueryResolver,
                BtVodEntryMatchingPredicates.schedulerChannelPredicate(KIDS_CATEGORY)
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

    private BtVodVersionsExtractor versionsExtractor(String prefix, String env, String conf) {
        return new BtVodVersionsExtractor(
                new BtVodPricingAvailabilityGrouper(),
                prefix,
                String.format(BT_VOD_GUID_ALIAS_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_ID_ALIAS_NAMESPACE_FORMAT, env, conf),
                btTvServiceId,
                btTvOtgServiceId
        );
    }

    private BtVodSeriesUriExtractor seriesUriExtractor(String prefix) {
        return new BtVodSeriesUriExtractor(brandUriExtractor(prefix));
    }
    
    public BtVodDescribedFieldsExtractor describedFieldsExtractor(
            Publisher publisher,
            String env,
            String conf,
            String baseUrl,
            String newFeedSuffix,
            String qParam,
            String btVodMpxProdFeedBaseUrlForGuidLookup
    ) {
        BtVodContentMatchingPredicate newContentPredicate = newFeedContentMatchingPredicate(btVodMpxProdFeedBaseUrlForGuidLookup, newFeedSuffix, qParam);
        return new BtVodDescribedFieldsExtractor(
                topicResolver,
                topicWriter,
                publisher,
                newContentPredicate,
                BtVodContentMatchingPredicates.schedulerChannelPredicate(KIDS_CATEGORY),
                BtVodContentMatchingPredicates.schedulerChannelPredicate(TV_CATEGORY),
                BtVodContentMatchingPredicates.schedulerChannelPredicate(SUBSCRIPTION_CATCHUP_SCHEDULER_CHANNEL),
                topicFor(feedNamepaceFor(env, conf), BT_VOD_NEW_FEED, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_KIDS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_TV_BOXSETS_TOPIC, publisher),
                topicFor(btVodAppCategoryNamespaceFor(env, conf), BT_VOD_CATCHUP_TOPIC, publisher),
                String.format(BT_VOD_GUID_ALIAS_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_ID_ALIAS_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_CONTENT_PROVIDER_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_GENRE_NAMESPACE_FORMAT, env, conf),
                String.format(BT_VOD_KEYWORD_NAMESPACE_FORMAT, env, conf)
        );
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
    
    public BtVodContentGroupUpdater btVodContentGroupUpdater(Publisher publisher, String uriPrefix, String baseUrl, String qParam,
            Map<String, BtVodContentMatchingPredicate> contentGroupsAndCriteria) {
        return new BtVodContentGroupUpdater(contentGroupResolver, contentGroupWriter, 
                contentGroupsAndCriteria, uriPrefix, publisher);
    }
    
    private BtVodData btVodData(String baseUrl, String feedName, String qParam) {
        return new BtVodData(
                mpxVodClient(baseUrl, feedName, qParam),
                feedName
        );
    }

    private HttpBtMpxVodClient mpxVodClient(String baseUrl, String itemLookupFeedName, String qParam) {
        return new HttpBtMpxVodClient(
                new SimpleHttpClientBuilder().withUserAgent(HttpClients.ATLAS_USER_AGENT)
                        .withConnectionTimeout(2, TimeUnit.MINUTES)
                        .build(),
                new HttpBtMpxFeedRequestProvider(baseUrl, itemLookupFeedName, qParam)
        );
    }
    
    private Map<String, BtVodContentMatchingPredicate> salesContentGroupsAndCriteria(String baseUrl, String feedName, String qParam) {
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
                .put(NEW_CATEGORY.toLowerCase(), BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(baseUrl, feedName, qParam), NEW_CONTENT_MPX_FEED_NAME))
                .build();
    }
    
    private Topic topicFor(String namespace, String topicName, Publisher publisher) {
        Topic topic = topicResolver.topicFor(publisher, namespace, topicName).requireValue();
        topicWriter.write(topic);
        return topic;
    }
    
    private BtVodContentMatchingPredicate newFeedContentMatchingPredicate(String baseUri, String feedName, String qParam) {
        return BtVodContentMatchingPredicates.mpxFeedContentMatchingPredicate(mpxVodClient(baseUri, feedName, qParam), NEW_CONTENT_MPX_FEED_NAME);
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
        scheduler.schedule(btVodUpdater(btVodMpxProdFeedNewSuffix, salesContentGroupsAndCriteria(btVodMpxProdFeedBaseUrlForGuidLookup, btVodMpxProdFeedName, btVodMpxProdFeedQParam)).withName("BT VoD Updater"), MPX_REPETITION_RULE);

        scheduleBtTveVodProdConfig1Updater();

        scheduler.schedule(btTveVodSystest2Config1Updater(), TVE_MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodVoldConfig1Updater(), TVE_MPX_REPETITION_RULE);
        scheduler.schedule(btTveVodVoleConfig1Updater(), TVE_MPX_REPETITION_RULE);
    }

    private void scheduleBtTveVodProdConfig1Updater() {
        for (LocalTime ingestTime : TVE_MPX_DAILY_INGEST_TIMES) {
            ScheduledTask task = btTveVodProdConfig1Updater();
            String name = task.getName() + " " + ingestTime.toString("HH:mm");

            scheduler.schedule(task.withName(name), RepetitionRules.daily(ingestTime));
        }
    }
}
