package org.atlasapi.query.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;


public class FilterActivelyPublishedOnlyQueryExecutor implements KnownTypeQueryExecutor {

    private final KnownTypeQueryExecutor delegate;

    public FilterActivelyPublishedOnlyQueryExecutor(KnownTypeQueryExecutor delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris, ContentQuery query) {
        return filter(delegate.executeUriQuery(uris, query));
    }

    @Override
    public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, ContentQuery query) {
        return filter(delegate.executeIdQuery(ids, query));
    }

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace,
            Iterable<String> values, ContentQuery query) {
        return filter(delegate.executeAliasQuery(namespace, values, query));
    }

    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers,
            ContentQuery query) {
        return filter(delegate.executePublisherQuery(publishers, query));
    }

    public static final Predicate<Identified> IS_ACTIVELY_PUBLISHED = new Predicate<Identified>() {
        @Override public boolean apply(Identified input) {
            if (input instanceof Described) {
                return ((Described) input).isActivelyPublished();
            }
            return true;
        }
    };

    public static final Predicate<List<?>> IS_NON_EMPTY = new Predicate<List<?>>(){
        @Override public boolean apply(List<?> input) {
            return ! input.isEmpty();
        }};

    public static Map<String, List<Identified>> filter(Map<String, List<Identified>> unfiltered) {
        return Maps.filterValues(
                Maps.transformValues(unfiltered, new Function<List<Identified>, List<Identified>>(){
                    @Override public List<Identified> apply(List<Identified> equivalatedItemSet) {
                        return Lists.newArrayList(Iterables.filter(equivalatedItemSet, IS_ACTIVELY_PUBLISHED));
                    }}),
                IS_NON_EMPTY
        );
    }

}
