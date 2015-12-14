package org.atlasapi.query.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;


public class FilterScheduleOnlyQueryExecutor implements KnownTypeQueryExecutor {
    
    private final KnownTypeQueryExecutor delegate;

    public FilterScheduleOnlyQueryExecutor(KnownTypeQueryExecutor delegate) {
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

    private Map<String, List<Identified>> filter(Map<String, List<Identified>> unfiltered) {
        return Maps.filterValues(unfiltered, new Predicate<List<Identified>>() {
            @Override
            public boolean apply(List<Identified> input) {
                for (Identified i : input) {
                    if (i instanceof Content && ((Content)i).isScheduleOnly()) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

}
