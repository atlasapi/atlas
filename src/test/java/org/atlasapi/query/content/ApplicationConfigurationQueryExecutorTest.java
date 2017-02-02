//package org.atlasapi.query.content;
//
//import java.util.List;
//import java.util.Map;
//
//import junit.framework.TestCase;
//
//import org.atlasapi.application.ApplicationConfiguration;
//import org.atlasapi.content.criteria.ContentQuery;
//import org.atlasapi.media.entity.Brand;
//import org.atlasapi.media.entity.Content;
//import org.atlasapi.media.entity.ContentGroup;
//import org.atlasapi.media.entity.Described;
//import org.atlasapi.media.entity.Episode;
//import org.atlasapi.media.entity.Identified;
//import org.atlasapi.media.entity.Item;
//import org.atlasapi.media.entity.Publisher;
//import org.atlasapi.persistence.content.ContentResolver;
//import org.atlasapi.persistence.content.ContentWriter;
//import org.atlasapi.persistence.content.mongo.MongoContentWriter;
//import org.atlasapi.persistence.lookup.NewLookupWriter;
//
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.metabroadcast.common.persistence.MongoTestHelper;
//import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
//import com.metabroadcast.common.time.Clock;
//import com.metabroadcast.common.time.SystemClock;
//
//public class ApplicationConfigurationQueryExecutorTest extends TestCase {
//
//    private final Clock clock = new SystemClock();
//    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
//    private NewLookupWriter lookupStore = new NewLookupWriter() {
//        @Override
//        public void ensureLookup(Described described) {
//        }
//    };
//    
//    private final ContentWriter writer = new MongoContentWriter(mongo, lookupStore , clock);
//    private final ContentResolver resolver = null;
//    
//    private final MongoDBQueryExecutor mongoQueryExecutor = new MongoDBQueryExecutor(resolver);
//    
//    private final ApplicationConfigurationQueryExecutor queryExecutor = new ApplicationConfigurationQueryExecutor(mongoQueryExecutor);
//
//    /* Config: C4
//     * Item: C4 (no versions)
//     * Result: C4 item (no versions)
//     */
//    public void donttestItemWithNoVersionsIsNotFiltered() {
//		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
//		
//		Episode uglyBettyC4 = new Episode("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
//		uglyBettyC4.setTitle("Ugly Betty Episode One");
//		
//		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
//		ubC4.setDescription("blah blah blah");
//		ubC4.setTitle("Ugly Betty");
////		ubC4.setContents(uglyBettyC4);
//		
//		writer.createOrUpdate(ubC4);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Content> results = null;//queryExecutor.discover(query);
//		
//		assertEquals(1, results.size());
//		
//		Brand foundBrand = (Brand) Iterables.getOnlyElement(results);
//		
//		assertEquals(Publisher.C4, foundBrand.getPublisher());
//		assertEquals(0, Iterables.getOnlyElement(foundBrand.getContents()).getVersions().size());
//    }
//    
//    /* Config: C4
//     * Item: C4
//     * 	versions: C4, Hulu
//     * Result: C4 item with only C4 version
//     */
////	public void testOnlyVersionsWithConfiguredProvidersPassFilter() {
////		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
////		
////		Episode uglyBettyC4 = new Episode("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
////		uglyBettyC4.setTitle("Ugly Betty Episode One");
////		Version c4Version = new Version();
////		c4Version.setProvider(Publisher.C4);
////		Version huluVersion = new Version();
////		huluVersion.setProvider(Publisher.HULU);
////		uglyBettyC4.setVersions(ImmutableSet.of(c4Version, huluVersion));
////		
////		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
////		ubC4.setDescription("blah blah blah");
////		ubC4.setTitle("Ugly Betty");
////		ubC4.setContents(uglyBettyC4);
////		
////		store.createOrUpdate(ubC4, true);
////		
////		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
////		
////		List<Content> results = queryExecutor.discover(query);
////		
////		assertEquals(1, results.size());
////		
////		Brand foundBrand = (Brand) Iterables.getOnlyElement(results);
////		
////		assertEquals(Publisher.C4, foundBrand.getPublisher());
////		for (Item item : foundBrand.getContents()) {
////			for (Version version : ((Item) item).getVersions()) {
////				assertEquals(Publisher.C4, version.getProvider());
////			}
////		}
////	}
//
//	
//    /* Config: C4
//     * Item: C4
//     * 	versions: BBC
//     * Result: One item
//     */
////	public void testItemPassesFilterWhereAllVersionsFailFilterWhenQueryDoesntSpecifyVersionOrBelow() {
////		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
////		
////		Episode uglyBettyC4 = new Episode("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
////		uglyBettyC4.setTitle("Ugly Betty Episode One");
////		Version bbcVersion = new Version();
////		bbcVersion.setProvider(Publisher.BBC);
////		uglyBettyC4.addVersion(bbcVersion);
////		
////		store.createOrUpdate(uglyBettyC4);
////		
////		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
////		
////		List<Content> results = queryExecutor.discover(query);
////		
////		assertEquals( 1, results.size());
////		assertEquals( 0, ((Item) results.get(0)).getVersions().size());
////		
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.BBC));
////		query = query.copyWithApplicationConfiguration(config);
////		
////		results = queryExecutor.discover(query);
////		
////		assertEquals(1, results.size() );
////		assertEquals(1, ((Item) results.get(0)).getVersions().size()); 
////	}
//
//	
//    /* Config: C4
//     * Item: C4
//     * 	versions: BBC
//     * Result: No items
//     */
////	public void testItemFailsFilterWhereAllVersionsFailFilterWhenQuerySpecifiesVersionOrBelow() {
////		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
////		
////		Episode uglyBettyC4 = new Episode("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
////		uglyBettyC4.setTitle("Ugly Betty Episode One");
////		Version bbcVersion = new Version();
////		bbcVersion.setDuration(Duration.standardHours(10500));
////		bbcVersion.setProvider(Publisher.BBC);
////		uglyBettyC4.addVersion(bbcVersion);
////		
////		store.createOrUpdate(uglyBettyC4);
////		
////		ContentQuery query = new ContentQuery(Attributes.VERSION_DURATION.createQuery(Operators.GREATER_THAN, ImmutableList.of(1))).copyWithApplicationConfiguration(config);
////		
////		List<Content> results = queryExecutor.discover(query);
////		
////		assertEquals( 0, results.size() );
////		
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.BBC));
////		query = query.copyWithApplicationConfiguration(config);
////		
////		assertEquals(1, queryExecutor.discover(query).size());
////	}
//
//	
//    public void donttestBrandWithNoItemsIsNotFiltered() {
//		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
//		
//		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
//		ubC4.setDescription("blah blah blah");
//		ubC4.setTitle("Ugly Betty");
//		
//		writer.createOrUpdate(ubC4);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Content> results = null;//queryExecutor.discover(query);
//		
//		assertEquals( 1, results.size());
//		for (Content brand : results) {
//			assertEquals(Publisher.C4, brand.getPublisher());
//			assertEquals(0, ((Brand) brand).getContents().size());
//		}
//    }
//    
//    public void donttestBrandWithBadPublisherIsFiltered() {
//		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.BBC));
//		
//		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
//		ubC4.setDescription("blah blah blah");
//		ubC4.setTitle("Ugly Betty");
//		
//		writer.createOrUpdate(ubC4);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Content> results = null;//queryExecutor.discover(query);
//		
//		assertEquals( 0, results.size());
//		
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.BBC));
//		query = query.copyWithApplicationConfiguration(config);
//		
////		results = queryExecutor.discover(query);
//		
//		assertEquals( 1, results.size() );
//    }
//    
//	public void donttestOnlyItemsWithConfiguredPublisherPassFilter() {
//		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
//		
//		Episode uglyBettyC4 = new Episode("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
//		uglyBettyC4.setTitle("Ugly Betty Episode One");
//		Episode uglyBettyHulu = new Episode("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.HULU);
//		uglyBettyHulu.setTitle("Ugly Betty Episode One");
//		
//		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
//		ubC4.setDescription("blah blah blah");
//		ubC4.setTitle("Ugly Betty");
////		ubC4.setContents(uglyBettyC4, uglyBettyHulu);
//		
//		writer.createOrUpdate(ubC4);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Content> results = null;//queryExecutor.discover(query);
//		
//		assertEquals( 1, results.size());
//		for (Content brand : results) {
//			assertEquals(Publisher.C4, brand.getPublisher());
//			for (Item item : ((Brand) brand).getContents()) {
//				assertEquals(Publisher.C4, item.getPublisher());
//			}
//		}
//	}
//	
//	public void donttestBrandPassesFilterWhereAllItemsFailFilterWhenQueryDoesntSpecifyItemOrBelow() {
//		ApplicationConfiguration config = includingPublishers(Publisher.C4);
//		
//		Episode uglyBettyHulu = new Episode("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.HULU);
//		
//		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
////		ubC4.setContents(uglyBettyHulu);
//		
//		writer.createOrUpdate(ubC4);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Content> results = null;//queryExecutor.discover(query);
//		
//		assertEquals( 1, results.size() );
//		assertEquals( 0, ((Brand) results.get(0)).getContents().size());
//		
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.HULU));
//		query = query.copyWithApplicationConfiguration(config);
//		
////		results = queryExecutor.discover(query);
//		
//		assertEquals( 1, results.size() );
//		assertEquals( 1, ((Brand) results.get(0)).getContents().size()); 
//	}
//	
////	public void testBrandFailsFilterWhereAllItemsFailFilterWhenQuerySpecifiesItemOrBelow() {
////		ApplicationConfiguration config = includingPublishers(Publisher.C4);
////		
////		Episode uglyBettyHulu = new Episode("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.C4);
////		uglyBettyHulu.setIsLongForm(true);
////		
////		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
////		ubC4.setGenres(ImmutableSet.of("Yawn"));
////		ubC4.setContents(uglyBettyHulu);
////		
//
////		writer.createOrUpdate(ubC4);
////		
////		ContentQuery query = new ContentQuery(Attributes.ITEM_IS_LONG_FORM.createQuery(Operators.EQUALS, ImmutableList.of(false))).copyWithApplicationConfiguration(config);
////		
////		List<Content> results = queryExecutor.discover(query);
////
////		assertEquals( 0, results.size() );
////		
////		query =  new ContentQuery(Attributes.ITEM_IS_LONG_FORM.createQuery(Operators.EQUALS, ImmutableList.of(true))).copyWithApplicationConfiguration(config);
////		assertEquals( 1, queryExecutor.discover(query).size() );
////	}
//	
////	public void testBrandPassesFilterWhereAllItemsFailFilterWhenQueryDoesntSpecifyVersionOrBelow() {
////		ApplicationConfiguration config = ApplicationConfiguration.DEFAULT_CONFIGURATION;
////		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4));
////		
////		Episode uglyBettyHulu = new Episode("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.C4);
////
////		Version bbcVersion = new Version();
////		bbcVersion.setProvider(Publisher.BBC);
////		uglyBettyHulu.addVersion(bbcVersion);
////	
////		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
////		ubC4.setContents(uglyBettyHulu);
////		
////		store.createOrUpdate(ubC4, true);
////		
////		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
////		
////		List<Content> results = queryExecutor.discover(query);
////		
////		Brand brand = (Brand) Iterables.getOnlyElement(results);
////		assertEquals(1, brand.getContents().size());
////		
////		Episode episode = Iterables.getOnlyElement(brand.getContents());
////		assertEquals(0, episode.getVersions().size());
////		
////		results = queryExecutor.discover(query.copyWithApplicationConfiguration(includingPublishers(Publisher.C4, Publisher.BBC)));
////		
////		brand = (Brand) Iterables.getOnlyElement(results);
////		episode = Iterables.getOnlyElement(brand.getContents());
////		assertEquals(1, episode.getVersions().size());
////	}
//
//	
////	public void testBrandFailsFilterWhereAllItemsFailFilterWhenQueryDoesSpecifyVersionOrBelow() {
////		
////		Episode uglyBettyHulu = new Episode("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.C4);
////		Version bbcVersion = new Version();
////		bbcVersion.setProvider(Publisher.BBC);
////		bbcVersion.setDuration(Duration.standardHours(1000));
////		uglyBettyHulu.addVersion(bbcVersion);
////	
////		Brand ubC4 = new Brand("http://www.channel4.com/uglybetty", "c4:ugly-betty", Publisher.C4);
////		ubC4.setContents(uglyBettyHulu);
////		
////		writer.createOrUpdate(ubC4);
////		
////		ContentQuery query = new ContentQuery(Attributes.VERSION_DURATION.createQuery(Operators.GREATER_THAN, ImmutableList.of(1))).copyWithApplicationConfiguration(includingPublishers(Publisher.C4));
////		
////		List<Content> results = queryExecutor.discover(query);
////		
////		assertEquals(0, results.size());
////		
////		results = queryExecutor.discover(query.copyWithApplicationConfiguration(includingPublishers(Publisher.C4, Publisher.BBC)));
////		
////		assertEquals(1, results.size());
////		
////		Brand brand = (Brand) results.get(0);
////		
////		assertEquals(1, brand.getContents().size()); 
////		assertEquals(1, brand.getContents().get(0).getVersions().size());
////	}
//	
//    public void donttestPlaylistWithNoItemsIsNotFiltered() {
//		ApplicationConfiguration config = includingPublishers(Publisher.C4);
//		
//		ContentGroup group = new ContentGroup("group", "group", Publisher.C4);
//		writer.createOrUpdateSkeleton(group);
//		
//		Map<String, List<Identified>> results = queryExecutor.executeUriQuery(ImmutableList.of("group"), ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config));
//		
//		assertEquals(1, results.size());
//		
////		ContentGroup foundGroup = (ContentGroup) Iterables.getOnlyElement(results);
////		assertEquals(Publisher.C4, foundGroup.getPublisher());
////		assertEquals(0, foundGroup.getContents().size());
//    }
//
//	private ApplicationConfiguration includingPublishers(Publisher... publishers) {
//		return ApplicationConfiguration.DEFAULT_CONFIGURATION.copyWithIncludedPublishers(ImmutableSet.copyOf(publishers));
//	}
//    
//    public void donttestPlaylistWithBadPublisherIsFiltered() {
//		ApplicationConfiguration config = includingPublishers(Publisher.BBC);
//		
//		ContentGroup group = new ContentGroup("group", "group", Publisher.C4);
//		writer.createOrUpdateSkeleton(group);
//		
//		ContentQuery query = ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config);
//		
//		List<Identified> results = null;//queryExecutor.executeUriQuery(ImmutableList.of("group"), ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config));
//		
//		assertEquals(0, results.size());
//		
//		config = config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.BBC));
//		query = query.copyWithApplicationConfiguration(config);
//		
////		results = queryExecutor.executeUriQuery(ImmutableList.of("group"), ContentQuery.MATCHES_EVERYTHING.copyWithApplicationConfiguration(config));
//		
//		assertEquals(1, results.size());
//    }
//    
////	public void testOnlyBrandsWithConfiguredPublisherPassFilter() {
////		ApplicationConfiguration config = includingPublishers(Publisher.C4);
////		
////		ContentGroup group = new ContentGroup("group", "group", Publisher.C4);
////		Brand c4playlist = new Brand("http://www.channel4.com/uglybetty/one", "c4:ugly-betty-one", Publisher.C4);
////		Brand huluPlaylist = new Brand("http://www.hulu.com/uglybetty/one", "hulu:ugly-betty-one", Publisher.HULU);
////		
////		group.setContents(c4playlist, huluPlaylist);
////		
////		store.createOrUpdate(huluPlaylist, true);
////		store.createOrUpdate(c4playlist, true);
////		store.createOrUpdateSkeleton(group);
////		
////		ContentQuery query = new ContentQuery(Attributes.DESCRIPTION_GENRE.createQuery(Operators.EQUALS, ImmutableSet.of("Fun"))).copyWithApplicationConfiguration(config);
////		
////		List<Identified> results = queryExecutor.executeUriQuery(ImmutableList.of("group"), query);
////		
////		assertEquals(1, results.size());
////		
////		ContentGroup found = (ContentGroup) Iterables.getOnlyElement(results);
////		assertEquals(Publisher.C4, found.getPublisher());
////		for (Content pl : found.getContents()) {
////			assertEquals(Publisher.C4, pl.getPublisher());
////		}
////	}
//
////	public void testPlaylistFailsFilterWhereAllSubPlaylistsFailFilterWhenQueryDoesSpecifyBrandOrBelow() {
////		ApplicationConfiguration config = includingPublishers(Publisher.C4);
////		
////		ContentGroup group = new ContentGroup("group", "group", Publisher.C4);
////		
////		Brand hulubrand = new Brand("http://www.hulu.com/uglybetty/one", "c4:ugly-betty-one", Publisher.HULU);
////		hulubrand.setGenres(ImmutableSet.of("Joy"));
////		
////		Episode huluEpisode = new Episode("http://www.hulu.com/uglybetty/one/one", "hulu:ugly-betty-one", Publisher.HULU);
////		huluEpisode.setGenres(ImmutableSet.of("Joy"));
////
////		Version huluVersion = new Version();
////		huluVersion.setDuration(Duration.standardHours(1000));
////		huluVersion.setProvider(Publisher.HULU);
////
////		huluEpisode.addVersion(huluVersion);
////		hulubrand.setContents(huluEpisode);
////		
////		group.setContents(hulubrand);
////		
////		store.createOrUpdate(hulubrand, true);
////		store.createOrUpdateSkeleton(group);
////		
////		ContentQuery query = new ContentQuery(Attributes.DESCRIPTION_GENRE.createQuery(Operators.EQUALS, ImmutableSet.of("Joy"))).copyWithApplicationConfiguration(config);
////		
////		List<Identified> results = queryExecutor.executeUriQuery(ImmutableList.of("group"), query);
////		
////		assertEquals(1, results.size());
////		assertEquals(0, ((ContentGroup) Iterables.getOnlyElement(results)).getContents().size());
////		
////		query = query.copyWithApplicationConfiguration(config.copyWithIncludedPublishers(ImmutableSet.of(Publisher.C4, Publisher.HULU)));
////		
////		results = queryExecutor.executeUriQuery(ImmutableList.of("group"), query);
////		
////		assertEquals(1, results.size());
////		ContentGroup foundGroup = (ContentGroup) results.get(0);
////		assertEquals(1, ((Container<?>) Iterables.getOnlyElement(foundGroup.getContents())).getContents().size());
////	}
//}
