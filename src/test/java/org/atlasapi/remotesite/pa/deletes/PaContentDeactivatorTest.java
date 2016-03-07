package org.atlasapi.remotesite.pa.deletes;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.metabroadcast.common.scheduling.StatusReporter;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.*;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.remotesite.pa.deletes.PaContentDeactivator;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaContentDeactivatorTest {

    private static final StatusReporter MOCK_REPORTER = new StatusReporter() {

        @Override
        public void reportStatus(String s) {
            System.out.println(s);
        }

        @Override
        public boolean shouldContinue() {
            return true;
        }
    };

    private PaContentDeactivator deactivator;
    private LookupEntryStore lookup;
    private ContentLister lister;
    private ContentWriter writer;
    private ScheduleTaskProgressStore progressStore;
    private Brand activeContainer;
    private Brand inactiveContainer;
    private Brand emptyContainer;
    private Brand emptyContainerButHasGenericChildren;
    private Item activeItem;
    private Item inactiveItem;
    private DBCollection childCollection;

    @Before
    public void setUp() throws Exception {
        lookup = mock(LookupEntryStore.class);
        lister = mock(ContentLister.class);
        writer = mock(ContentWriter.class);
        progressStore = mock(ScheduleTaskProgressStore.class);
        childCollection = mock(DBCollection.class);

        deactivator = new PaContentDeactivator(
                lister,
                writer,
                progressStore,
                childCollection
        );

        activeContainer = new Brand("http://pressassociation.com/series/10", "10", Publisher.PA);
        activeContainer.setChildRefs(
                ImmutableList.of(
                        new ChildRef(10l, "", "", DateTime.now(), EntityType.CLIP)
                )
        );
        activeContainer.setId(10l);

        inactiveContainer = new Brand("http://pressassociation.com/series/20", "20", Publisher.PA);
        inactiveContainer.setId(20l);
        inactiveContainer.setGenericDescription(true);

        emptyContainer = new Brand("http://pressassociation.com/series/30", "30", Publisher.PA);
        emptyContainer.setId(30l);

        emptyContainerButHasGenericChildren = new Brand("http://pressassociation.com/brand/40", "40", Publisher.PA);
        emptyContainerButHasGenericChildren.setId(40l);

        Item genericChildOfEmptyBrand = new Item("40children", "40children", Publisher.PA);
        genericChildOfEmptyBrand.setGenericDescription(true);
        genericChildOfEmptyBrand.setContainer(emptyContainerButHasGenericChildren);
        writer.createOrUpdate(genericChildOfEmptyBrand);

        activeItem = new Item("http://pressassociation.com/episodes/50", "50", Publisher.PA);
        activeItem.setId(50l);
        inactiveItem = new Item("http://pressassociation.com/episodes/60", "60", Publisher.PA);
        inactiveItem.setId(60l);

        setupMocks(
                activeItem,
                inactiveItem,
                activeContainer,
                inactiveContainer,
                emptyContainer,
                emptyContainerButHasGenericChildren
        );
    }

    @Test
    public void testDateExtractionFromFileName() {
        DateTime dateTime = deactivator.extractCutoffTimeFromFilename("20151228_archiveID.xml");
        assertThat(dateTime.getYear(), is(2015));
        assertThat(dateTime.getMonthOfYear(), is(12));
        assertThat(dateTime.getDayOfMonth(), is(28));
    }

    @Test
    public void testDeactivatesContentCorrectly() throws Exception {
        SetMultimap<String, String> typesToIds = MultimapBuilder.SetMultimapBuilder
                .hashKeys()
                .hashSetValues()
                .build();

        typesToIds.put("pa:brand", "10");
        typesToIds.put("pa:episode", "50");
        deactivator.deactivate(typesToIds, false, MOCK_REPORTER, DateTime.now());
        Thread.sleep(2000);
        assertThat(activeItem.isActivelyPublished(), is(true));
        assertThat(inactiveItem.isActivelyPublished(), is(false));

        assertThat(activeContainer.isActivelyPublished(), is(true));

        assertThat(emptyContainerButHasGenericChildren.isActivelyPublished(), is(true));
        assertThat(inactiveContainer.isActivelyPublished(), is(false));
        assertThat(emptyContainer.isActivelyPublished(), is(false));
    }

    private void setupMocks(
            Item activeItem,
            Item inactiveItem,
            Brand activeContainer,
            Brand inactiveContainer,
            Brand emptyContainer,
            Brand emptyContainerWithGenericChildren) {
        LookupEntry activeLookup = mock(LookupEntry.class);
        when(activeLookup.id()).thenReturn(10l).thenReturn(50l);
        DBCursor dbCursor = mock(DBCursor.class);
        when(childCollection.find(any(DBObject.class))).thenReturn(dbCursor);
        when(dbCursor.count()).thenReturn(0).thenReturn(0).thenReturn(1);

        when(lookup.entriesForAliases(any(Optional.class), anyListOf(String.class)))
                .thenReturn(ImmutableList.of(activeLookup));

        when(progressStore.progressForTask(PaContentDeactivator.class.getSimpleName()+"containers"))
                .thenReturn(null);
        when(progressStore.progressForTask(PaContentDeactivator.class.getSimpleName()+"children"))
                .thenReturn(null);

        ImmutableList<ContentCategory> childCats = ImmutableList.of(
                ContentCategory.CHILD_ITEM,
                ContentCategory.TOP_LEVEL_ITEM
        );

        Iterator<Content> childrenItr = ImmutableList.<Content>of(activeItem, inactiveItem).iterator();

        ContentListingCriteria childCriteria = ContentListingCriteria.defaultCriteria()
                .forContent(childCats)
                .forPublishers(Publisher.PA)
                .build();

        when(lister.listContent(childCriteria)).thenReturn(childrenItr);

        ImmutableList<ContentCategory> containerCat = ImmutableList.of(
                ContentCategory.CONTAINER
        );

        Iterator<Content> containerItr = ImmutableList.<Content>of(
                activeContainer, inactiveContainer, emptyContainer, emptyContainerWithGenericChildren).iterator();

        ContentListingCriteria containerCriteria = ContentListingCriteria.defaultCriteria()
                .forContent(containerCat)
                .forPublishers(Publisher.PA)
                .build();

        when(lister.listContent(containerCriteria)).thenReturn(containerItr);
    }

}