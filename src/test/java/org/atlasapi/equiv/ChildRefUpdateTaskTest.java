package org.atlasapi.equiv;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.Iterator;

import junit.framework.TestCase;

import org.atlasapi.equiv.update.tasks.MongoScheduleTaskProgressStore;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.ReadPreference;

public class ChildRefUpdateTaskTest extends TestCase {

    DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    PersistenceAuditLog persistenceAuditLog = new NoLoggingPersistenceAuditLog();
    ScheduleTaskProgressStore progressStore = new MongoScheduleTaskProgressStore(mongo);
    MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(mongo.collection("lookup"), 
            persistenceAuditLog, ReadPreference.primary());
    ContentResolver resolver = new LookupResolvingContentResolver(new MongoContentResolver(mongo, lookupStore), lookupStore);
    
    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
    
    ContentWriter writer = new MongoContentWriter(mongo, lookupStore, persistenceAuditLog, playerResolver, serviceResolver, new SystemClock());
    ContentLister lister = new MongoContentLister(mongo);
    
    ChildRefUpdateTask task = new ChildRefUpdateTask(lister, resolver, mongo, progressStore).forPublishers(BBC);
    
    ContainerTranslator containerTranslator = new ContainerTranslator(new SubstitutionTableNumberCodec());
    ItemTranslator itemTranslator = new ItemTranslator(new SubstitutionTableNumberCodec());
    
    @Before
    public void clear() {
        mongo.collection("containers").drop();
        mongo.collection("programmeGroups").drop();
        mongo.collection("children").drop();
    }
    
    @Test
    public void testUpdatesParentChildReferencesInBrandAndSeries() {
        
        Brand brand = new Brand("brandUri", "cBrandUri", BBC);
        Series series1 = seriesWithParent("series1Uri", brand).withSeriesNumber(1);
        Series series2 = seriesWithParent("series2Uri", brand).withSeriesNumber(2);
        Episode episode1 = episodeWithContainers("ep1", 1, series1, brand);
        Item item1 = itemWithContainer("item1", brand);
        Episode episode2 = episodeWithContainers("ep2", 2, series2, brand);

        write(brand, series1, series2, episode1, item1, episode2);
        
        setId("containers", brand.getCanonicalUri(), 1L);
        setId("programmeGroups", series1.getCanonicalUri(), 2L);
        setId("programmeGroups", series2.getCanonicalUri(), 3L);
        setId("children", episode1.getCanonicalUri(), 4L);
        setId("children", item1.getCanonicalUri(), 5L);
        setId("children", episode2.getCanonicalUri(), 6L);
        
        Brand resolvedBrand = resolve(brand);
        assertThat(resolvedBrand.getId(), is(1L));
        checkSeriesIds(resolvedBrand.getSeriesRefs(), null, null);
        checkSeriesNumbers(resolvedBrand.getSeriesRefs(), 2, 1);
        checkIds(resolvedBrand.getChildRefs(), null, null, null);
        
        checkSeries(resolve(series1), 2L, null, null, null);
        checkSeries(resolve(series2), 3L, null, (Long)null);
        
        checkEpisode(resolve(episode1), 4L, null, null);
        checkEpisode(resolve(item1), 5L, null, null);
        checkEpisode(resolve(episode2), 6L, null, null);
        
        task.run();
        
        resolvedBrand = resolve(brand);
        assertThat(resolvedBrand.getId(), is(1L));
        checkSeriesIds(resolvedBrand.getSeriesRefs(), 3L, 2L);
        checkSeriesNumbers(resolvedBrand.getSeriesRefs(), 2, 1);
        checkIds(resolvedBrand.getChildRefs(), 5L, 6L, 4L);
        
        checkSeries(resolve(series1), 2L, 1L, 4L, 5L);
        checkSeries(resolve(series2), 3L, 1L, 6L);
        
        checkEpisode(resolve(episode1), 4L, 1L, 2L);
        checkEpisode(resolve(item1), 5L, 1L, null);
        checkEpisode(resolve(episode2), 6L, 1L, 3L);
    }

    private void write(Content...contents) {
        for (Content content : contents) {
            if (content instanceof Container) {
                writer.createOrUpdate((Container) content);
            }
            if (content instanceof Item) {
                writer.createOrUpdate((Item) content);
            }
            
        }
    }

    @Test
    public void testUpdatesReferencesInTopLevelSeries() {
        Series series = seriesWithParent("seriesUri", null);
        Episode episode1 = episodeWithContainers("ep1", 1, series, series);
        Item item1 = itemWithContainer("ep2", series);
        
        write(series, episode1, item1);
        
        setId("containers", series.getCanonicalUri(), 1L);
        setId("children", episode1.getCanonicalUri(), 2L);
        setId("children", item1.getCanonicalUri(), 3L);
        
        checkTopLevelSeries(resolve(series), 1L, null, null);
        checkEpisode(resolve(episode1), 2L, null, null);
        checkEpisode(resolve(item1), 3L, null, null);
        
        task.run();
        
        checkTopLevelSeries(resolve(series), 1L, 3L, 2L);
        checkEpisode(resolve(episode1), 2L, 1L, 1L);
        checkEpisode(resolve(item1), 3L, 1L, null);
        
    }

    private void checkEpisode(Item resolvedItem, Long episodeId, Long brandId, Long seriesId) {
        assertThat(resolvedItem.getId(), is(episodeId));
        assertThat(resolvedItem.getContainer().getId(), is(brandId));
        if (resolvedItem instanceof Episode) {
            ParentRef seriesRef = ((Episode)resolvedItem).getSeriesRef();
            if (seriesRef != null) {
                assertThat(seriesRef.getId(), is(seriesId));
            } else {
                assertThat(seriesId, is(nullValue()));
            }
        }
    }

    private void checkSeries(Series resolvedSeries, long seriesId, Long brandId, Long...childIds) {
        checkTopLevelSeries(resolvedSeries, seriesId, childIds);
        assertThat(resolvedSeries.getParent().getId(), is(brandId));
    }

    private void checkTopLevelSeries(Series resolvedSeries, long seriesId, Long...childIds) {
        assertThat(resolvedSeries.getId(), is(seriesId));
        checkIds(resolvedSeries.getChildRefs(), childIds);
    }

    private void checkIds(ImmutableList<ChildRef> childRefs, Long... ids) {
        Iterator<Long> idIter = Lists.newArrayList(ids).iterator();
        for (ChildRef childRef : childRefs) {
            assertThat(childRef.toString(), childRef.getId(), is(idIter.next()));
        }
    }
    
    private void checkSeriesIds(ImmutableList<SeriesRef> seriesRefs, Long... ids) {
        Iterator<Long> idIter = Lists.newArrayList(ids).iterator();
        for (SeriesRef seriesRef : seriesRefs) {
            assertThat(seriesRef.toString(), seriesRef.getId(), is(idIter.next()));
        }
    }
    

    private void checkSeriesNumbers(ImmutableList<SeriesRef> seriesRefs, Integer...numbers) {
        Iterator<Integer> idIter = Lists.newArrayList(numbers).iterator();
        for (SeriesRef seriesRef : seriesRefs) {
            assertThat(seriesRef.toString(), seriesRef.getSeriesNumber(), is(idIter.next()));
        }
    }
    
    private Item itemWithContainer(String uri, Container container) {
        Item item = new Item(uri, "c"+uri, BBC);
        if (container != null) {
            item.setContainer(container);
        }
        return item;
    }

    private Episode episodeWithContainers(String uri, Integer episodeNumber, Series series, 
            Container container) {
        Episode episode1 = new Episode(uri, "c"+uri, BBC);
        episode1.setContainer(container);
        if (series != null) {
            episode1.setSeries(series);
        }
        episode1.setEpisodeNumber(episodeNumber);
        return episode1;
    }

    private Series seriesWithParent(String uri, final Brand brand) {
        final Series series;
        series = new Series(uri, "c"+uri, BBC);
        if (brand != null) {
            series.setParent(brand);
        }
        return series;
    }

    private void setId(String collection, String id, long aid) {
        mongo.collection(collection).update(where().idEquals(id).build(), update().setField("aid", aid).build());
        mongo.collection("lookup").update(where().idEquals(id).build(), update().setField("aid", aid).build());
    }

    @SuppressWarnings("unchecked")
    private <T extends Content> T resolve(T content) {
        String uri = content.getCanonicalUri();
        return (T) resolver.findByCanonicalUris(ImmutableList.of(uri))
            .get(uri).requireValue();
    }

}
