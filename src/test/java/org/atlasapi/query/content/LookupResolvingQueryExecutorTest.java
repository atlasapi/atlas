package org.atlasapi.query.content;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.InMemoryLookupEntryStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;

@RunWith(JMock.class)
public class LookupResolvingQueryExecutorTest extends TestCase {
    private final Mockery context = new Mockery();

    private KnownTypeContentResolver cassandraContentResolver = context.mock(KnownTypeContentResolver.class, "cassandraContentResolver");
    private KnownTypeContentResolver mongoContentResolver = context.mock(KnownTypeContentResolver.class, "mongoContentResolver");

    private final InMemoryLookupEntryStore lookupStore = new InMemoryLookupEntryStore();

    private final LookupResolvingQueryExecutor executor = new LookupResolvingQueryExecutor(cassandraContentResolver, mongoContentResolver, lookupStore, true);

    @Test
    public void testSetsSameAs() {
        final String query = "query";
        final Item queryItem = new Item(query, "qcurie", Publisher.BBC);
        final Item enabledEquivItem = new Item("eequiv", "eecurie", Publisher.YOUTUBE);
        final Item disabledEquivItem = new Item("dequiv", "decurie", Publisher.PA);

        writeEquivalenceEntries(queryItem, enabledEquivItem, disabledEquivItem);

        context.checking(new Expectations(){{
            one(mongoContentResolver).findByLookupRefs(with(hasItems(LookupRef.from(queryItem), LookupRef.from(enabledEquivItem))));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).put(enabledEquivItem.getCanonicalUri(), enabledEquivItem).build()));
        }});
        context.checking(new Expectations(){{
            never(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                MatchesNothing.asQuery().copyWithApplicationConfiguration(
                        ApplicationConfiguration.defaultConfiguration()
                                .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                                .withSource(Publisher.YOUTUBE, SourceStatus.AVAILABLE_ENABLED)
                )
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

        lookupStore.store(LookupEntry.lookupEntryFrom(queryItem));

        context.checking(new Expectations(){{
            one(mongoContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).build()));
        }});
        context.checking(new Expectations(){{
            never(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                MatchesNothing.asQuery().copyWithApplicationConfiguration(
                        ApplicationConfiguration.defaultConfiguration()
                                .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                )
        );

        assertEquals(1, result.get(query).size());

        context.assertIsSatisfied();
    }

    @Test
    public void testCassandraIsCalledIfMongoReturnsNothing() {
        final String query = "query";
        final Item queryItem = new Item(query, "qcurie", Publisher.BBC);

        context.checking(new Expectations(){{
            never(mongoContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
        }});
        context.checking(new Expectations(){{
            one(cassandraContentResolver).findByLookupRefs(with(Expectations.<Iterable<LookupRef>>anything()));
            will(returnValue(ResolvedContent.builder().put(queryItem.getCanonicalUri(), queryItem).build()));
        }});

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                MatchesNothing.asQuery().copyWithApplicationConfiguration(
                        ApplicationConfiguration.defaultConfiguration()
                                .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                ));

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

        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(uri1, uri2), MatchesNothing.asQuery().copyWithApplicationConfiguration(ApplicationConfiguration.defaultConfiguration()));

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

        ApplicationConfiguration configWithoutPaEnabled = ApplicationConfiguration.defaultConfiguration();
        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                MatchesNothing.asQuery().copyWithApplicationConfiguration(configWithoutPaEnabled));

        assertTrue(result.isEmpty());

    }

    @Test
    public void testContentFromDisabledPublisherIsNotReturnedButEnabledEquivalentIs() {
        final String query = "query";
        final Item queryItem = item(1L, query, Publisher.PA);
        final Item equivItem = item(2L, "equiv", Publisher.BBC);

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

        ApplicationConfiguration configWithoutPaEnabled = ApplicationConfiguration.defaultConfiguration()
                .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                .copyWithPrecedence(ImmutableList.of(Publisher.BBC));
        Map<String, List<Identified>> result = executor.executeUriQuery(ImmutableList.of(query),
                MatchesNothing.asQuery().copyWithApplicationConfiguration(configWithoutPaEnabled));

        Identified mergedResult = result.get(query).get(0);
        assertThat(mergedResult, is((Identified)equivItem));
        assertThat(mergedResult.getEquivalentTo().size(), is(2));
    }

    private Item item(Long id, String query, Publisher source) {
        Item item = new Item(query, query+"curie", source);
        item.setId(id);
        return item;
    }
}