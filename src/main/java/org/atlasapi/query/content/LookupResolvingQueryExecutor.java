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

public class LookupResolvingQueryExecutor implements KnownTypeQueryExecutor {

    private final Logger log = LoggerFactory.getLogger(LookupResolvingQueryExecutor.class);

    private final KnownTypeQueryExecutor delegate;

    public LookupResolvingQueryExecutor(KnownTypeQueryExecutor executor) {
        this.delegate = executor;
    }

    @Override
    public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris,
            ContentQuery query) {
        return delegate.executeUriQuery(uris, query);
    }

    @Override
    public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids,
            ContentQuery query) {
        return delegate.executeIdQuery(ids, query);
    }

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace,
            Iterable<String> values, ContentQuery query) {
        return delegate.executeAliasQuery(namespace, values, query);
    }

    @Override
    public Map<String, List<Identified>> executePublisherQuery(
            Iterable<Publisher> publishers, ContentQuery query) {
        return delegate.executePublisherQuery(publishers, query);
    }

    @Override
    public Map<String, List<Identified>> executeEventQuery(Iterable<Long> eventIds,
            ContentQuery query) {
        return delegate.executeEventQuery(eventIds, query);
    }
}
