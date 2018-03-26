package org.atlasapi.query.content;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.atlasapi.content.criteria.AtomicQuery;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.InMemoryLookupEntryStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JMock.class)
public class LookupResolvingQueryExecutorTest extends TestCase {
    private final Mockery context = new Mockery();

    private KnownTypeContentResolver cassandraContentResolver = context.mock(KnownTypeContentResolver.class, "cassandraContentResolver");
    private KnownTypeContentResolver mongoContentResolver = context.mock(KnownTypeContentResolver.class, "mongoContentResolver");

    private final InMemoryLookupEntryStore lookupStore = new InMemoryLookupEntryStore();

    private final DefaultEquivalentContentResolver defaultEquivalentContentResolver =
            new DefaultEquivalentContentResolver(
                    mongoContentResolver,
                    lookupStore
            );
    private final LookupResolvingQueryExecutor executor = new LookupResolvingQueryExecutor(cassandraContentResolver, defaultEquivalentContentResolver, lookupStore, true);

    private Application defaultApplication = mock(Application.class);
    private ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
    private ContentQuery contentQuery = new ContentQuery(
            Collections.<AtomicQuery>singleton(MatchesNothing.get()),
            Selection.ALL,
            defaultApplication
    );

    @Before
    public void setUp() {
        when(configuration.getEnabledReadSources())
                .thenReturn(Publisher.all().stream()
                        .filter(Publisher::enabledWithNoApiKey)
                        .collect(MoreCollectors.toImmutableSet())
                );
        when(defaultApplication.getConfiguration()).thenReturn(configuration);
    }

    @Test
    public void testSetsSameAs() {
        final String query = "query";
        final Item queryItem = new Item(query, "qcurie", Publisher.BBC);
        final Item enabledEquivItem = new Item("eequiv", "eecurie", Publisher.YOUTUBE);
        final Item disabledEquivItem = new Item("dequiv", "decurie", Publisher.PA);
        when(defaultApplication.getConfiguration()).thenReturn(configurationWithReads(Publisher.BBC, Publisher.YOUTUBE));

        writeEquivalenceEntries(queryItem, enabledEquivItem, disabledEquivItem);

        context.checking(new Expectations(){{
            one(mongoContentResolver).findByLookupRefs(with(hasItems(LookupRef.from(queryItem), LookupRef.from(enabledEquivItem))));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).put(enabledEquivItem.getCanonicalUri(), enabledEquivItem).build()));
        }});
        context.checking(new Expectations(){{
            never(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                contentQuery
        );

        assertEquals(2, result.get(query).size());
        ImmutableSet<LookupRef> expectedEquivs = ImmutableSet.of(
                LookupRef.from(queryItem),
                LookupRef.from(enabledEquivItem),
                LookupRef.from(disabledEquivItem)
        );
        for (Identified resolved : result.get(query)) {
            assertEquals(expectedEquivs, resolved.getEquivalentTo());
        }

        context.assertIsSatisfied();
    }

    private void writeEquivalenceEntries(Item... items) {
        ImmutableSet<LookupRef> refs = ImmutableSet.copyOf(Iterables.transform(ImmutableList.copyOf(items), LookupRef.FROM_DESCRIBED));
        for (Item item : items) {
            lookupStore.store(LookupEntry.lookupEntryFrom(item)
                    .copyWithDirectEquivalents(refs)
                    .copyWithEquivalents(refs));
        }
    }

    @Test
    public void testCassandraIsNotCalledIfMongoReturnsSomething() {
        final String query = "query";
        final Item queryItem = new Item(query, "qcurie", Publisher.BBC);

        when(defaultApplication.getConfiguration()).thenReturn(configurationWithReads(Publisher.BBC));

        lookupStore.store(LookupEntry.lookupEntryFrom(queryItem));

        context.checking(new Expectations(){{
            one(mongoContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).build()));
        }});
        context.checking(new Expectations(){{
            never(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                contentQuery
        );

        assertEquals(1, result.get(query).size());

        context.assertIsSatisfied();
    }

    @Test
    public void testCassandraIsCalledIfMongoReturnsNothing() {
        final String query = "query";
        final Item queryItem = new Item(query, "qcurie", Publisher.BBC);

        when(defaultApplication.getConfiguration()).thenReturn(configurationWithReads(Publisher.BBC));

        context.checking(new Expectations(){{
            never(mongoContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});
        context.checking(new Expectations(){{
            one(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).build()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                contentQuery
        );

        assertEquals(1, result.get(query).size());

        context.assertIsSatisfied();
    }

    @Test
    public void testPublisherFilteringWithCassandra() {
        final String uri1 = "uri1";
        final Item item1 = new Item(uri1, "qcurie1", Publisher.BBC);
        final String uri2 = "uri2";
        final Item item2 = new Item(uri2, "qcurie1", Publisher.BBC);

        context.checking(new Expectations(){{
            never(mongoContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});
        context.checking(new Expectations(){{
            one(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(item1.getCanonicalUri(), item1).put(item2.getCanonicalUri(), item2).build()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(uri1, uri2), contentQuery);

        assertEquals(0, result.size());
        context.assertIsSatisfied();
    }

    @Test
    public void testContentFromDisabledPublisherIsNotReturned() {
        final String query = "query";
        final Item queryItem = item(1L, query, Publisher.PA);

        LookupEntry queryEntry = LookupEntry.lookupEntryFrom(queryItem);
        lookupStore.store(queryEntry);

        context.checking(new Expectations(){{
            never(mongoContentResolver).findByLookupRefs(with(hasItems(LookupRef.from(queryItem))));
        }});
        context.checking(new Expectations(){{
            one(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).build()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                contentQuery);

        assertTrue(result.isEmpty());

    }

    @Test
    public void testContentFromDisabledPublisherIsNotReturnedButEnabledEquivalentIs() {
        final String query = "query";
        final Item queryItem = item(1L, query, Publisher.PA);
        final Item equivItem = item(2L, "equiv", Publisher.BBC);

        when(defaultApplication.getConfiguration()).thenReturn(configurationWithReads(Publisher.BBC));

        LookupEntry queryEntry = LookupEntry.lookupEntryFrom(queryItem);
        LookupEntry equivEntry = LookupEntry.lookupEntryFrom(equivItem);

        lookupStore.store(queryEntry
                .copyWithDirectEquivalents(ImmutableSet.of(equivEntry.lookupRef()))
                .copyWithEquivalents(ImmutableSet.of(equivEntry.lookupRef())));
        lookupStore.store(equivEntry
                .copyWithDirectEquivalents(ImmutableSet.of(queryEntry.lookupRef()))
                .copyWithDirectEquivalents(ImmutableSet.of(queryEntry.lookupRef())));

        context.checking(new Expectations(){{
            one(mongoContentResolver).findByLookupRefs(with(hasItems(LookupRef.from(equivItem))));
            will(returnValue(ResolvedContent.builder().put(equivItem.getCanonicalUri(), equivItem).build()));
        }});
        context.checking(new Expectations(){{
            never(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                contentQuery
        );

        Identified mergedResult = result.get(query).get(0);
        assertThat(mergedResult, is((Identified)equivItem));
        assertThat(mergedResult.getEquivalentTo().size(), is(2));
    }

    private Item item(Long id, String query, Publisher source) {
        Item item = new Item(query, query+"curie", source);
        item.setId(id);
        return item;
    }

    private ApplicationConfiguration configurationWithReads(Publisher... publishers) {
        return ApplicationConfiguration.builder()
                .withPrecedence(Arrays.asList(publishers))
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }
}
