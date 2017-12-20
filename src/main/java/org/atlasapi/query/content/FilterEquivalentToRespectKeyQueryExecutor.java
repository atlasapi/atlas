package org.atlasapi.query.content;

import java.util.List;
import java.util.Map;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.query.content.FilterActivelyPublishedOnlyQueryExecutor.IS_ACTIVELY_PUBLISHED;
import static org.atlasapi.query.content.FilterActivelyPublishedOnlyQueryExecutor.IS_NON_EMPTY;

/**
 * Changes the equivalence (and thus the same_as list) to content only from publishers enabled in
 * the api_key, IF AND ONLY IF, the equivalent annotation is set.
 */
public class FilterEquivalentToRespectKeyQueryExecutor implements KnownTypeQueryExecutor {

    private final KnownTypeQueryExecutor delegate;

    public FilterEquivalentToRespectKeyQueryExecutor(KnownTypeQueryExecutor delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris,
            ContentQuery query) {
        return filter(delegate.executeUriQuery(uris, query), query);
    }

    @Override
    public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, ContentQuery query) {
        return filter(delegate.executeIdQuery(ids, query), query);
    }

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace,
            Iterable<String> values, ContentQuery query) {
        return filter(delegate.executeAliasQuery(namespace, values, query), query);
    }

    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers,
            ContentQuery query) {
        return filter(delegate.executePublisherQuery(publishers, query), query);
    }

    public static Map<String, List<Identified>> filter(
            Map<String, List<Identified>> content,
            ContentQuery query) {
        if (!query.getAnnotations().contains(Annotation.RESPECT_API_KEY_FOR_EQUIV_LIST)) {
            return content;
        }

        ImmutableSet<Publisher> enabledReadSources = query.getApplication()
                .getConfiguration()
                .getEnabledReadSources();

        for (Map.Entry<String, List<Identified>> entry : content.entrySet()) {
            for (Identified identified : entry.getValue()) {
                ImmutableSet<LookupRef> newSet =
                        identified.getEquivalentTo().stream()
                                .filter(i -> enabledReadSources.contains(i.publisher()))
                                .collect(MoreCollectors.toImmutableSet());
                identified.setEquivalentTo(newSet);
            }
        }

        return content;
    }

}
