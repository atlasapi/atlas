package org.atlasapi.equiv.query;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.equiv.OutputContentMerger;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MergeOnOutputQueryExecutor implements KnownTypeQueryExecutor {

    private final KnownTypeQueryExecutor delegate;
    
    private final OutputContentMerger merger = new OutputContentMerger();

    public MergeOnOutputQueryExecutor(KnownTypeQueryExecutor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris, final ContentQuery query) {
        return mergeResults(query, delegate.executeUriQuery(uris, query));
    }

    @Override
    public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, final ContentQuery query) {
        return mergeResults(query, delegate.executeIdQuery(ids, query));
    }

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace, Iterable<String> values,
            ContentQuery query) {
        return mergeResults(query, delegate.executeAliasQuery(namespace, values, query));
    }
    
    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers,
            ContentQuery query) {
        return mergeResults(query, delegate.executePublisherQuery(publishers, query));
    }

    private Map<String, List<Identified>> mergeResults(final ContentQuery query, Map<String, List<Identified>> unmergedResult) {
        final ApplicationConfiguration config = query.getConfiguration();
        if (!config.precedenceEnabled()) {
            return unmergedResult;
        }
        return Maps.transformValues(unmergedResult, new Function<List<Identified>, List<Identified>>() {

            @Override
            public List<Identified> apply(List<Identified> input) {

                List<Content> content = Lists.newArrayList();
                List<Identified> ids = Lists.newArrayList();

                for (Identified ided : input) {
                    if (ided instanceof Content) {
                        content.add((Content) ided);
                    } else {
                        ids.add(ided);
                    }
                }

                return ImmutableList.copyOf(Iterables.concat(merger.merge(config, content), ids));
            }
        });
    }

}
