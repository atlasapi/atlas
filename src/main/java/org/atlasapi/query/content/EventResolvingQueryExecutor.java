package org.atlasapi.query.content;


import java.util.List;
import java.util.Map;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContent;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.mongo.MongoTopLevelItemsEntry;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.metabroadcast.common.query.Selection;

public class EventResolvingQueryExecutor implements KnownTypeQueryExecutor {

    private final Logger log = LoggerFactory.getLogger(EventResolvingQueryExecutor.class);
    private final KnownTypeContentResolver cassandraContentResolver;
    private final boolean cassandraEnabled;
    private final EquivalentContentResolver mongoEquivsResolver;
    private final LookupEntryStore lookupEntryStore;
    private final MongoTopLevelItemsEntry itemsEntry;

    public EventResolvingQueryExecutor(KnownTypeContentResolver cassandraContentResolver,
            KnownTypeContentResolver mongoContentResolver,
            LookupEntryStore mongoLookupResolver,
            boolean cassandraEnabled,
            MongoTopLevelItemsEntry itemsEntry) {
        this.cassandraContentResolver = cassandraContentResolver;
        this.mongoEquivsResolver = new DefaultEquivalentContentResolver(mongoContentResolver, mongoLookupResolver);
        this.cassandraEnabled = cassandraEnabled;
        this.lookupEntryStore = mongoLookupResolver;
        this.itemsEntry = itemsEntry;

    }

    @Override
    public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris, final ContentQuery query) {
        EquivalentContent mongoResolved = mongoEquivsResolver.resolveUris(uris, query.getConfiguration(), query.getAnnotations(), true);
        Map<String, List<Identified>> results = Multimaps.asMap(ArrayListMultimap.<String, Identified>create(mongoResolved));
        if (cassandraEnabled && results.isEmpty()) {
            try {
                results = resolveCassandraEntries(uris, query);
            }
            catch(Exception e) {
                log.error(String.format("Cassandra resolution failed for URIS %s", uris), e);
            }
        }
        return ImmutableMap.copyOf(results);
    }

    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers, final ContentQuery query) {
        Iterable<LookupEntry> entries = lookupEntryStore.entriesForPublishers(publishers, query.getSelection());
        return executeUriQuery(Iterables.transform(entries, LookupEntry.TO_ID), query.copyWithSelection(
                Selection.all()));
    }

    @Override
    public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, final ContentQuery query) {
        EquivalentContent mongoResolved = mongoEquivsResolver.resolveIds(ids, query.getConfiguration(), query.getAnnotations());
        Map<String, List<Identified>> mongoResults = Multimaps.asMap(ArrayListMultimap.<String, Identified>create(mongoResolved));
        return ImmutableMap.copyOf(mongoResults);
    }

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace, Iterable<String> values,
            ContentQuery query) {
        EquivalentContent mongoResolved = mongoEquivsResolver.resolveAliases(namespace, values, query.getConfiguration(), query.getAnnotations());
        Map<String, List<Identified>> mongoResults = Multimaps.asMap(ArrayListMultimap.<String, Identified>create(mongoResolved));
        return ImmutableMap.copyOf(mongoResults);
    }

    @Override
    public Map<String, List<Identified>> executeEventQuery(Iterable<Long> eventIds,
            ContentQuery query) {
        return executeUriQuery(itemsEntry.getItemUrisForEventIds(eventIds), query);
    }

    private Map<String, List<Identified>> resolveCassandraEntries(Iterable<String> uris, ContentQuery query) {
        final ApplicationConfiguration configuration = query.getConfiguration();
        ResolvedContent result = cassandraContentResolver.findByLookupRefs(Iterables.transform(uris, new Function<String, LookupRef>() {

            @Override
            public LookupRef apply(String input) {
                return new LookupRef(input, null, null, null);
            }
        }));
        return Maps.transformValues(Maps.filterValues(result.asResolvedMap(), new Predicate<Identified>() {

            @Override
            public boolean apply(Identified input) {
                return ((input instanceof Described)
                        && configuration.isEnabled(((Described) input).getPublisher()));
            }
        }), new Function<Identified, List<Identified>>() {

            @Override
            public List<Identified> apply(Identified input) {
                return ImmutableList.of(input);
            }
        });
    }

}
