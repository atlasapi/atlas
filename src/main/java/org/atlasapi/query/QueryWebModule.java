package org.atlasapi.query;

import com.google.common.base.Splitter;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.equiv.OutputChannelMerger;
import org.atlasapi.feeds.tasks.Task;
import org.atlasapi.feeds.tasks.persistence.TaskStore;
import org.atlasapi.feeds.tasks.simple.TaskQueryResult;
import org.atlasapi.feeds.tvanytime.TvAnytimeGenerator;
import org.atlasapi.feeds.utils.DescriptionWatermarker;
import org.atlasapi.feeds.utils.WatermarkModule;
import org.atlasapi.feeds.youview.ContentHierarchyExpanderFactory;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsQueryResult;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsResolver;
import org.atlasapi.input.ChannelGroupTransformer;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.DefaultJacksonModelReader;
import org.atlasapi.input.ImageModelTransformer;
import org.atlasapi.input.PersonModelTransformer;
import org.atlasapi.input.TopicModelTransformer;
import org.atlasapi.media.channel.CachingChannelGroupStore;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.ChannelQueryResult;
import org.atlasapi.media.entity.simple.ContentGroupQueryResult;
import org.atlasapi.media.entity.simple.ContentQueryResult;
import org.atlasapi.media.entity.simple.EventQueryResult;
import org.atlasapi.media.entity.simple.PeopleQueryResult;
import org.atlasapi.media.entity.simple.ProductQueryResult;
import org.atlasapi.media.entity.simple.ScheduleQueryResult;
import org.atlasapi.media.entity.simple.TopicQueryResult;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.DispatchingAtlasModelWriter;
import org.atlasapi.output.JaxbTVAnytimeModelWriter;
import org.atlasapi.output.JaxbXmlTranslator;
import org.atlasapi.output.JsonTranslator;
import org.atlasapi.output.MergingChannelModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.output.SimpleChannelGroupModelWriter;
import org.atlasapi.output.SimpleChannelModelWriter;
import org.atlasapi.output.SimpleContentGroupModelWriter;
import org.atlasapi.output.SimpleContentModelWriter;
import org.atlasapi.output.SimpleEventModelWriter;
import org.atlasapi.output.SimpleFeedStatisticsModelWriter;
import org.atlasapi.output.SimplePersonModelWriter;
import org.atlasapi.output.SimpleProductModelWriter;
import org.atlasapi.output.SimpleScheduleModelWriter;
import org.atlasapi.output.SimpleTaskModelWriter;
import org.atlasapi.output.SimpleTopicModelWriter;
import org.atlasapi.output.TransformingModelWriter;
import org.atlasapi.output.rdf.RdfXmlTranslator;
import org.atlasapi.output.simple.ChannelGroupModelSimplifier;
import org.atlasapi.output.simple.ChannelGroupSimplifier;
import org.atlasapi.output.simple.ChannelGroupSummarySimplifier;
import org.atlasapi.output.simple.ChannelModelSimplifier;
import org.atlasapi.output.simple.ChannelNumberingChannelGroupModelSimplifier;
import org.atlasapi.output.simple.ChannelNumberingChannelModelSimplifier;
import org.atlasapi.output.simple.ChannelNumberingsChannelGroupToChannelModelSimplifier;
import org.atlasapi.output.simple.ChannelNumberingsChannelToChannelGroupModelSimplifier;
import org.atlasapi.output.simple.ChannelRefSimplifier;
import org.atlasapi.output.simple.ChannelSimplifier;
import org.atlasapi.output.simple.ContainerModelSimplifier;
import org.atlasapi.output.simple.ContentGroupModelSimplifier;
import org.atlasapi.output.simple.EventModelSimplifier;
import org.atlasapi.output.simple.EventRefModelSimplifier;
import org.atlasapi.output.simple.FeedStatisticsModelSimplifier;
import org.atlasapi.output.simple.ImageSimplifier;
import org.atlasapi.output.simple.ItemModelSimplifier;
import org.atlasapi.output.simple.OrganisationModelSimplifier;
import org.atlasapi.output.simple.PersonModelSimplifier;
import org.atlasapi.output.simple.PlayerModelSimplifier;
import org.atlasapi.output.simple.ProductModelSimplifier;
import org.atlasapi.output.simple.PublisherSimplifier;
import org.atlasapi.output.simple.ResponseModelSimplifier;
import org.atlasapi.output.simple.ServiceModelSimplifier;
import org.atlasapi.output.simple.TaskModelSimplifier;
import org.atlasapi.output.simple.TopicModelSimplifier;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EquivalenceContentWriter;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventContentLister;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.persistence.output.MongoAvailableItemsResolver;
import org.atlasapi.persistence.output.MongoContainerSummaryResolver;
import org.atlasapi.persistence.output.MongoRecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.output.MongoUpcomingItemsResolver;
import org.atlasapi.persistence.output.RecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.topic.PublisherFilteringTopicContentLister;
import org.atlasapi.query.topic.PublisherFilteringTopicResolver;
import org.atlasapi.query.v2.ChannelController;
import org.atlasapi.query.v2.ChannelGroupController;
import org.atlasapi.query.v2.ChannelGroupWriteExecutor;
import org.atlasapi.query.v2.ChannelWriteExecutor;
import org.atlasapi.query.v2.ContentFeedController;
import org.atlasapi.query.v2.ContentGroupController;
import org.atlasapi.query.v2.ContentWriteController;
import org.atlasapi.query.v2.EventsController;
import org.atlasapi.query.v2.FeedStatsController;
import org.atlasapi.query.v2.PeopleController;
import org.atlasapi.query.v2.PeopleWriteController;
import org.atlasapi.query.v2.ProductController;
import org.atlasapi.query.v2.QueryController;
import org.atlasapi.query.v2.ScheduleController;
import org.atlasapi.query.v2.SearchController;
import org.atlasapi.query.v2.TaskController;
import org.atlasapi.query.v2.TopicController;
import org.atlasapi.query.v2.TopicWriteController;
import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.remotesite.util.OldContentDeactivator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import tva.metadata._2010.TVAMainType;

import javax.xml.bind.JAXBElement;

import static org.atlasapi.persistence.MongoContentPersistenceModule.NON_ID_SETTING_CONTENT_WRITER;
import static org.atlasapi.persistence.MongoContentPersistenceModule.NO_EQUIVALENCE_WRITING_CONTENT_WRITER;
import static org.atlasapi.persistence.MongoContentPersistenceModule.EXPLICIT_LOOKUP_WRITER;

@Configuration
@Import({ WatermarkModule.class, QueryExecutorModule.class })
public class QueryWebModule {

    @Value("${local.host.name}") private String localHostName;
    @Value("${ids.expose}") private String exposeIds;
    @Value("${events.whitelist.ids}") private String eventsWhitelist;

    @Autowired private DatabasedMongo mongo;
    @Autowired private ContentGroupWriter contentGroupWriter;
    @Autowired private ContentGroupResolver contentGroupResolver;
    @Autowired @Qualifier(NON_ID_SETTING_CONTENT_WRITER) private EquivalenceContentWriter contentWriter;
    @Autowired @Qualifier(NO_EQUIVALENCE_WRITING_CONTENT_WRITER) private ContentWriter noEquivalenceWritingContentWriter;
    @Autowired private LookupBackedContentIdGenerator lookupBackedContentIdGenerator;
    @Autowired private ScheduleWriter scheduleWriter;
    @Autowired private ContentResolver contentResolver;
    @Autowired private ChannelResolver channelResolver;
    @Autowired private ChannelGroupStore channelGroupStore;
    @Autowired private ScheduleResolver scheduleResolver;
    @Autowired private SearchResolver searchResolver;
    @Autowired private PeopleResolver peopleResolver;
    @Autowired private TopicQueryResolver topicResolver;
    @Autowired @Qualifier("topicStore")  private TopicStore topicStore;
    @Autowired private TopicContentLister topicContentLister;
    @Autowired private SegmentResolver segmentResolver;
    @Autowired private ProductResolver productResolver;
    @Autowired private PeopleQueryResolver peopleQueryResolver;
    @Autowired private PersonStore personStore;
    @Autowired private ServiceResolver serviceResolver;
    @Autowired private PlayerResolver playerResolver;
    @Autowired private LookupEntryStore lookupStore;
    @Autowired private DescriptionWatermarker descriptionWatermarker;
    @Autowired private EventResolver eventResolver;
    @Autowired private FeedStatisticsResolver feedStatsResolver;
    @Autowired private TvAnytimeGenerator feedGenerator;
    @Autowired private LastUpdatedContentFinder contentFinder;
    @Autowired private SegmentWriter segmentWriter;
    @Autowired private TaskStore taskStore;
    @Autowired private ContentHierarchyExpanderFactory hierarchyExpanderFactory;
    @Autowired private ChannelStore channelStore;
    @Autowired private LookupEntryStore entryStore;
    @Autowired private LookupWriter lookupWriter;
    @Autowired @Qualifier(EXPLICIT_LOOKUP_WRITER) private LookupWriter explicitLookupWriter;

    @Autowired private KnownTypeQueryExecutor queryExecutor;
    @Autowired @Qualifier("YouviewQueryExecutor")  private KnownTypeQueryExecutor allowMultipleFromSamePublisherExecutor;
    @Autowired private ApplicationFetcher applicationFetcher;
    @Autowired private AdapterLog log;
    @Autowired private EventContentLister eventContentLister;

    @Autowired private ContentWriteExecutor contentWriteExecutor;
    @Autowired private MessageSender<ContentWriteMessage> contentWriteMessageSender;

    @Bean
    ChannelController channelController() {
        return new ChannelController(
                applicationFetcher,
                log,
                channelModelWriter(),
                channelResolver,
                new SubstitutionTableNumberCodec(),
                channelWriteExecutor()
        );
    }

    private ChannelWriteExecutor channelWriteExecutor() {
        return ChannelWriteExecutor.builder()
                .withAppConfigFetcher(applicationFetcher)
                .withChannelStore(channelStore)
                .withModelReader(new DefaultJacksonModelReader())
                .withChannelTransformer(
                        ChannelModelTransformer.create(
                                v4ChannelCodec(),
                                ImageModelTransformer.create()
                        )
                )
                .withOutputter(channelModelWriter())
                .build();
    }

    @Bean
    AtlasModelWriter<Iterable<Channel>> channelModelWriter() {
        ChannelModelSimplifier channelModelSimplifier = channelModelSimplifier();

        return this.standardWriter(
                modelWriterFor(new JsonTranslator<>(), channelModelSimplifier),
                modelWriterFor(new JaxbXmlTranslator<>(), channelModelSimplifier)
        );
    }

    private TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> modelWriterFor(
            AtlasModelWriter<ChannelQueryResult> modelWriter,
            ChannelModelSimplifier simplifier
    ) {
        return MergingChannelModelWriter.builder()
                .withChannelResolver(channelResolver)
                .withMerger(OutputChannelMerger.create())
                .withDelegate(new SimpleChannelModelWriter(modelWriter, simplifier))
                .withQueryResultModelWriter(modelWriter)
                .build();
    }

    @Bean
    ChannelModelSimplifier channelModelSimplifier() {
        return new ChannelModelSimplifier(
                channelSimplifier(),
                channelNumberingsChannelToChannelGroupModelSimplifier()
        );
    }

    @Bean
    ChannelSimplifier channelSimplifier() {
        return new ChannelSimplifier(v3ChannelCodec(),
                v4ChannelCodec(),
                channelResolver,
                publisherSimplifier(),
                imageSimplifier(),
                channelGroupAliasSimplifier(),
                channelRefSimplifier(),
                new CachingChannelGroupStore(channelGroupStore)
        );
    }

    @Bean ChannelRefSimplifier channelRefSimplifier() {
        return new ChannelRefSimplifier(v3ChannelCodec());
    }

    @Bean
    ChannelGroupSummarySimplifier channelGroupAliasSimplifier() {
        return new ChannelGroupSummarySimplifier(v3ChannelCodec(), cachingChannelGroupResolver());
    }

    @Bean
    ChannelNumberingsChannelToChannelGroupModelSimplifier channelNumberingsChannelToChannelGroupModelSimplifier() {
        return new ChannelNumberingsChannelToChannelGroupModelSimplifier(
                cachingChannelGroupResolver(),
                new ChannelNumberingChannelGroupModelSimplifier(channelGroupSimplifier())
        );
    }

    @Bean
    ChannelGroupSimplifier channelGroupSimplifier() {
        return new ChannelGroupSimplifier(
                new SubstitutionTableNumberCodec(),
                cachingChannelGroupResolver(),
                publisherSimplifier()
        );
    }

    @Bean
    ChannelGroupResolver cachingChannelGroupResolver() {
        return new CachingChannelGroupStore(channelGroupStore);
    }

    @Bean
    ImageSimplifier imageSimplifier() {
        return new ImageSimplifier();
    }

    @Bean
    PlayerModelSimplifier playerSimplifier() {
        return new PlayerModelSimplifier(imageSimplifier());
    }

    @Bean
    ServiceModelSimplifier serviceSimplifier() {
        return new ServiceModelSimplifier(imageSimplifier());
    }

    private SubstitutionTableNumberCodec v3ChannelCodec() {
        return new SubstitutionTableNumberCodec();
    }

    private SubstitutionTableNumberCodec v4ChannelCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @Bean
    ChannelGroupController channelGroupController() {
        return ChannelGroupController.builder()
                .withApplicationFetcher(applicationFetcher)
                .withLog(log)
                .withAtlasModelWriter(channelGroupModelWriter())
                .withModelReader(new DefaultJacksonModelReader())
                .withChannelGroupResolver(cachingChannelGroupResolver())
                .withChannelGroupTransformer(new ChannelGroupTransformer())
                .withChannelGroupWriteExecutor(channelGroupWriteExecutor())
                .withChannelResolver(channelResolver)
                .build();
    }

    private ChannelGroupWriteExecutor channelGroupWriteExecutor() {
        return ChannelGroupWriteExecutor.create(channelGroupStore, channelStore);
    }

    @Bean
    AtlasModelWriter<Iterable<ChannelGroup>> channelGroupModelWriter() {
        ChannelGroupModelSimplifier channelGroupModelSimplifier = ChannelGroupModelSimplifier();
        return this.standardWriter(
                new SimpleChannelGroupModelWriter(
                        new JsonTranslator<>(),
                        channelGroupModelSimplifier
                ),
                new SimpleChannelGroupModelWriter(
                        new JaxbXmlTranslator<>(),
                        channelGroupModelSimplifier
                )
        );
    }

    @Bean
    ChannelGroupModelSimplifier ChannelGroupModelSimplifier() {
        return new ChannelGroupModelSimplifier(channelGroupSimplifier(), numberingSimplifier());
    }

    @Bean
    ChannelNumberingsChannelGroupToChannelModelSimplifier numberingSimplifier() {
        return new ChannelNumberingsChannelGroupToChannelModelSimplifier(
                channelResolver,
                new ChannelNumberingChannelModelSimplifier(channelSimplifier())
        );
    }

    @Bean
    PublisherSimplifier publisherSimplifier() {
        return new PublisherSimplifier();
    }

    @Bean
    QueryController queryController() {
        return new QueryController(
                queryExecutor,
                allowMultipleFromSamePublisherExecutor,
                applicationFetcher,
                log,
                contentModelOutputter(),
                contentWriteController(),
                eventContentLister
        );
    }

    private NumberToShortStringCodec idCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    private ContentWriteController contentWriteController() {
        return ContentWriteController.create(
                applicationFetcher,
                contentWriteExecutor,
                lookupBackedContentIdGenerator,
                contentWriteMessageSender,
                contentModelOutputter(),
                lookupStore,
                contentResolver,
                contentWriter,
                noEquivalenceWritingContentWriter,
                EquivalenceBreaker.create(
                        contentResolver,
                        entryStore,
                        lookupWriter,
                        explicitLookupWriter
                ),
                OldContentDeactivator.create(
                        new MongoContentLister(
                                mongo,
                                new MongoContentResolver(mongo, lookupStore)
                        ),
                        contentWriter,
                        contentResolver
                )
        );
    }

    TopicWriteController topicWriteController() {
        return new TopicWriteController(
                applicationFetcher,
                topicStore,
                new DefaultJacksonModelReader(),
                new TopicModelTransformer(),
                topicModelOutputter()
        );
    }

    @Bean
    ScheduleController schedulerController() {
        return new ScheduleController(
                scheduleResolver,
                channelResolver,
                applicationFetcher,
                log,
                scheduleChannelModelOutputter(),
                DefaultApplication.createDefault()
        );
    }

    @Bean
    PeopleController peopleController() {
        return new PeopleController(
                peopleQueryResolver,
                applicationFetcher,
                log,
                personModelOutputter(),
                peopleWriteController(),
                DefaultApplication.createDefault()
        );
    }

    private PeopleWriteController peopleWriteController() {
        return new PeopleWriteController(
                applicationFetcher,
                personStore,
                new DefaultJacksonModelReader(),
                new PersonModelTransformer(new SystemClock(), ImageModelTransformer.create(), personStore),
                personModelOutputter()
        );
    }

    @Bean
    SearchController searchController() {
        return new SearchController(searchResolver, applicationFetcher, log, contentModelOutputter());
    }

    @Bean
    TopicController topicController() {
        return new TopicController(
                new PublisherFilteringTopicResolver(topicResolver),
                new PublisherFilteringTopicContentLister(topicContentLister),
                applicationFetcher,
                log,
                topicModelOutputter(),
                queryController(),
                topicWriteController()
        );
    }

    @Bean
    ProductController productController() {
        return new ProductController(
                productResolver,
                queryExecutor,
                applicationFetcher,
                log,
                productModelOutputter(),
                queryController()
        );
    }

    @Bean
    ContentGroupController contentGroupController() {
        return new ContentGroupController(
                contentGroupResolver,
                queryExecutor,
                applicationFetcher,
                log,
                contentGroupOutputter(),
                queryController()
        );
    }

    @Bean
    EventsController eventController() {
        Iterable<String> whitelistedIds = Splitter.on(',').trimResults().omitEmptyStrings()
                .split(eventsWhitelist);
        return new EventsController(
                applicationFetcher,
                log,
                eventModelOutputter(),
                idCodec(),
                eventResolver,
                topicResolver,
                whitelistedIds
        );
    }

    @Bean
    TaskController taskController() {
        return new TaskController(applicationFetcher, log, taskModelOutputter(), taskStore, idCodec());
    }

    @Bean
    FeedStatsController feedStatsController() {
        return new FeedStatsController(
                applicationFetcher,
                log,
                feedStatsModelOutputter(),
                feedStatsResolver
        );
    }

    @Bean
    ContentFeedController contentFeedController() {
        return new ContentFeedController(
                applicationFetcher,
                log,
                tvaModelOutputter(),
                feedGenerator,
                contentResolver,
                channelResolver,
                hierarchyExpanderFactory
        );
    }

    @Bean
    AtlasModelWriter<QueryResult<Identified, ? extends Identified>> contentModelOutputter() {
        return this.standardWriter(
                new SimpleContentModelWriter(
                        new JsonTranslator<ContentQueryResult>(),
                        contentItemModelSimplifier(),
                        containerSimplifier(),
                        topicSimplifier(),
                        productSimplifier(),
                        imageSimplifier(),
                        personSimplifier()
                ),
                new SimpleContentModelWriter(
                        new JaxbXmlTranslator<ContentQueryResult>(),
                        contentItemModelSimplifier(),
                        containerSimplifier(),
                        topicSimplifier(),
                        productSimplifier(),
                        imageSimplifier(),
                        personSimplifier()
                )
        );
    }

    @Bean
    ContainerModelSimplifier containerSimplifier() {
        RecentlyBroadcastChildrenResolver recentChildren = new MongoRecentlyBroadcastChildrenResolver(
                mongo);
        NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
        ContainerSummaryResolver containerSummary = new MongoContainerSummaryResolver(
                mongo,
                idCodec
        );
        ContainerModelSimplifier containerSimplier = new ContainerModelSimplifier(
                contentItemModelSimplifier(),
                localHostName,
                contentGroupResolver,
                topicResolver,
                availableItemsResolver(),
                upcomingItemsResolver(),
                productResolver,
                recentChildren,
                imageSimplifier(),
                peopleQueryResolver,
                containerSummary,
                eventRefSimplifier()
        );
        containerSimplier.exposeIds(Boolean.valueOf(exposeIds));
        return containerSimplier;
    }

    @Bean
    EventRefModelSimplifier eventRefSimplifier() {
        return new EventRefModelSimplifier(eventSimplifier(), eventResolver, idCodec());
    }

    @Bean
    EventModelSimplifier eventSimplifier() {
        return new EventModelSimplifier(
                topicSimplifier(),
                personSimplifier(),
                organisationSimplifier(),
                idCodec()
        );
    }

    @Bean
    TaskModelSimplifier taskSimplifier() {
        return new TaskModelSimplifier(idCodec(), new ResponseModelSimplifier());
    }

    @Bean
    FeedStatisticsModelSimplifier feedStatsSimplifier() {
        return new FeedStatisticsModelSimplifier();
    }

    @Bean
    OrganisationModelSimplifier organisationSimplifier() {
        return new OrganisationModelSimplifier(imageSimplifier(), personSimplifier(), idCodec());
    }

    @Bean
    PersonModelSimplifier personSimplifier() {
        return new PersonModelSimplifier(
                imageSimplifier(),
                upcomingItemsResolver(),
                availableItemsResolver()
        );
    }

    @Bean
    MongoUpcomingItemsResolver upcomingItemsResolver() {
        return new MongoUpcomingItemsResolver(mongo);
    }

    @Bean
    MongoAvailableItemsResolver availableItemsResolver() {
        return new MongoAvailableItemsResolver(mongo, lookupStore);
    }

    @Bean
    ItemModelSimplifier contentItemModelSimplifier() {
        return itemModelSimplifier(false);
    }

    @Bean
    ItemModelSimplifier scheduleItemModelSimplifier() {
        return itemModelSimplifier(true);
    }

    private ItemModelSimplifier itemModelSimplifier(boolean withWatermark) {
        NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
        NumberToShortStringCodec channelIdCodec = new SubstitutionTableNumberCodec();
        ContainerSummaryResolver containerSummary = new MongoContainerSummaryResolver(
                mongo,
                idCodec
        );
        DescriptionWatermarker watermarker = withWatermark ? descriptionWatermarker : null;
        ItemModelSimplifier itemSimplifier = new ItemModelSimplifier(localHostName,
                contentGroupResolver,
                topicResolver,
                productResolver,
                segmentResolver,
                containerSummary,
                channelResolver,
                idCodec,
                channelIdCodec,
                imageSimplifier(),
                peopleQueryResolver,
                upcomingItemsResolver(),
                availableItemsResolver(),
                watermarker,
                playerResolver,
                playerSimplifier(),
                channelSimplifier(),
                serviceResolver,
                serviceSimplifier(),
                eventRefSimplifier()
        );
        itemSimplifier.exposeIds(Boolean.valueOf(exposeIds));
        return itemSimplifier;
    }

    @Bean
    AtlasModelWriter<Iterable<Person>> personModelOutputter() {
        return this.standardWriter(
                new SimplePersonModelWriter(
                        new JsonTranslator<PeopleQueryResult>(),
                        imageSimplifier(),
                        upcomingItemsResolver(),
                        availableItemsResolver()
                ),
                new SimplePersonModelWriter(
                        new JaxbXmlTranslator<PeopleQueryResult>(),
                        imageSimplifier(),
                        upcomingItemsResolver(),
                        availableItemsResolver()
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<ScheduleChannel>> scheduleChannelModelOutputter() {
        return this.standardWriter(
                new SimpleScheduleModelWriter(
                        new JsonTranslator<ScheduleQueryResult>(),
                        scheduleItemModelSimplifier(),
                        channelSimplifier()
                ),
                new SimpleScheduleModelWriter(
                        new JaxbXmlTranslator<ScheduleQueryResult>(),
                        scheduleItemModelSimplifier(),
                        channelSimplifier()
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<Topic>> topicModelOutputter() {
        TopicModelSimplifier topicModelSimplifier = topicSimplifier();
        return this.standardWriter(
                new SimpleTopicModelWriter(
                        new JsonTranslator<TopicQueryResult>(),
                        contentResolver,
                        topicModelSimplifier
                ),
                new SimpleTopicModelWriter(
                        new JaxbXmlTranslator<TopicQueryResult>(),
                        contentResolver,
                        topicModelSimplifier
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<Event>> eventModelOutputter() {
        EventModelSimplifier eventModelSimplifier = eventSimplifier();
        return this.standardWriter(
                new SimpleEventModelWriter(
                        new JsonTranslator<EventQueryResult>(),
                        contentResolver,
                        eventModelSimplifier
                ),
                new SimpleEventModelWriter(
                        new JaxbXmlTranslator<EventQueryResult>(),
                        contentResolver,
                        eventModelSimplifier
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<Task>> taskModelOutputter() {
        TaskModelSimplifier taskModelSimplifier = taskSimplifier();
        return this.standardWriter(
                new SimpleTaskModelWriter(
                        new JsonTranslator<TaskQueryResult>(),
                        taskModelSimplifier
                ),
                new SimpleTaskModelWriter(
                        new JaxbXmlTranslator<TaskQueryResult>(),
                        taskModelSimplifier
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<FeedStatistics>> feedStatsModelOutputter() {
        FeedStatisticsModelSimplifier feedStatsSimplifier = feedStatsSimplifier();
        return this.standardWriter(
                new SimpleFeedStatisticsModelWriter(
                        new JsonTranslator<FeedStatisticsQueryResult>(),
                        feedStatsSimplifier
                ),
                new SimpleFeedStatisticsModelWriter(
                        new JaxbXmlTranslator<FeedStatisticsQueryResult>(),
                        feedStatsSimplifier
                )
        );
    }

    @Bean
    AtlasModelWriter<JAXBElement<TVAMainType>> tvaModelOutputter() {
        AtlasModelWriter<JAXBElement<TVAMainType>> jaxbWriter = new JaxbTVAnytimeModelWriter();
        return DispatchingAtlasModelWriter.<JAXBElement<TVAMainType>>dispatchingModelWriter()
                .register(jaxbWriter, "xml", MimeType.APPLICATION_XML)
                .build();
    }

    @Bean
    AtlasModelWriter<Iterable<Product>> productModelOutputter() {
        ProductModelSimplifier modelSimplifier = productSimplifier();
        return this.standardWriter(
                new SimpleProductModelWriter(
                        new JsonTranslator<ProductQueryResult>(),
                        contentResolver,
                        modelSimplifier
                ),
                new SimpleProductModelWriter(
                        new JaxbXmlTranslator<ProductQueryResult>(),
                        contentResolver,
                        modelSimplifier
                )
        );
    }

    @Bean
    AtlasModelWriter<Iterable<ContentGroup>> contentGroupOutputter() {
        ContentGroupModelSimplifier modelSimplifier = contentGroupSimplifier();
        return this.standardWriter(
                new SimpleContentGroupModelWriter(
                        new JsonTranslator<ContentGroupQueryResult>(),
                        modelSimplifier
                ),
                new SimpleContentGroupModelWriter(
                        new JaxbXmlTranslator<ContentGroupQueryResult>(),
                        modelSimplifier
                )
        );
    }

    @Bean
    ContentGroupModelSimplifier contentGroupSimplifier() {
        return new ContentGroupModelSimplifier(imageSimplifier());
    }

    @Bean
    TopicModelSimplifier topicSimplifier() {
        return new TopicModelSimplifier(localHostName);
    }

    @Bean
    ProductModelSimplifier productSimplifier() {
        return new ProductModelSimplifier(localHostName);
    }

    private <I extends Iterable<?>> AtlasModelWriter<I> standardWriter(
            AtlasModelWriter<I> jsonWriter, AtlasModelWriter<I> xmlWriter) {
        return DispatchingAtlasModelWriter.<I>dispatchingModelWriter().register(
                new RdfXmlTranslator<I>(),
                "rdf.xml",
                MimeType.APPLICATION_RDF_XML
        )
                .register(jsonWriter, "json", MimeType.APPLICATION_JSON)
                .register(xmlWriter, "xml", MimeType.APPLICATION_XML)
                .build();
    }
}
