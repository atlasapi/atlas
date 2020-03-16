package org.atlasapi.query.content.search;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import org.atlasapi.deer.elasticsearch.ContentTitleSearcher;
import org.atlasapi.deer.elasticsearch.SearchResults;
import org.atlasapi.deer.elasticsearch.content.Id;
import org.atlasapi.deer.elasticsearch.content.Specialization;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DeerSearchResolver implements SearchResolver {

    private final ContentTitleSearcher searcher;
    private ContentResolver contentResolver;
    private LookupEntryStore lookupEntryStore;
    private final long timeout;

    public DeerSearchResolver(
            ContentTitleSearcher searcher,
            long timeout,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore
    ) {
        this.searcher = checkNotNull(searcher);
        this.timeout = timeout;
        this.contentResolver = contentResolver;
        this.lookupEntryStore = lookupEntryStore;
    }

    @Override
    public List<Identified> search(
            org.atlasapi.search.model.SearchQuery owlQuery,
            Application application
    ) {
        org.atlasapi.deer.elasticsearch.SearchQuery.Builder deerSearchQuery =
                new org.atlasapi.deer.elasticsearch.SearchQuery.Builder(owlQuery.getTerm());

        if (owlQuery.getSelection() != null) {
            deerSearchQuery.withSelection(owlQuery.getSelection());
        }

        if (owlQuery.getIncludedSpecializations() != null) {
            deerSearchQuery.withSpecializations(getDeerSpecializations(owlQuery.getIncludedSpecializations()));
        }

        if (owlQuery.getIncludedPublishers() != null) {
            deerSearchQuery.withPublishers(owlQuery.getIncludedPublishers());
        }

        if (owlQuery.type() != null) {
            deerSearchQuery.withType(owlQuery.type());
        }

        if (owlQuery.currentBroadcastsOnly() != null) {
            deerSearchQuery.withCurrentBroadcastsOnly(owlQuery.currentBroadcastsOnly());
        }

        if (owlQuery.topLevelOnly() != null) {
            deerSearchQuery.isTopLevelOnly(owlQuery.topLevelOnly());
        }

        deerSearchQuery.withTitleWeighting(owlQuery.getTitleWeighting());
        deerSearchQuery.withBroadcastWeighting(owlQuery.getBroadcastWeighting());
        deerSearchQuery.withCatchupWeighting(owlQuery.getCatchupWeighting());
        deerSearchQuery.withPriorityChannelWeighting(owlQuery.getPriorityChannelWeighting());

        return search(deerSearchQuery.build());
    }

    public List<Identified> search(org.atlasapi.deer.elasticsearch.SearchQuery query) {
        try {
            SearchResults results = searcher.search(query).get(timeout, TimeUnit.MILLISECONDS);
            return resolveIds(results.getIds());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private List<Identified> resolveIds(List<Id> ids) {
        Set<LookupEntry> potentialCandidatesSet = ids.stream()
                .map(this::getLookupEntries) //from atlas
                .filter(Objects::nonNull)
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolved = contentResolver.findByCanonicalUris(potentialCandidatesSet.stream()
                .map(LookupEntry::uri)
                .collect(MoreCollectors.toImmutableSet()));

        return resolved.getAllResolvedResults();
    }

    private Set<Specialization> getDeerSpecializations(
            Set<org.atlasapi.media.entity.Specialization> owlSpecializations) {

        return owlSpecializations.stream()
                .map(org.atlasapi.media.entity.Specialization::toString)
                .map(Specialization::fromKey)
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .collect(Collectors.toSet());
    }

    private Iterable<LookupEntry> getLookupEntries(Id id) {
        return lookupEntryStore.entriesForIds(ImmutableSet.of(id.longValue()));
    }

    public void setContentResolver(ContentResolver contentResolver) {
        this.contentResolver = contentResolver;
    }

    public void setLookupEntryStore(LookupEntryStore lookupEntryStore) {
        this.lookupEntryStore = lookupEntryStore;
    }
}
