package org.atlasapi.query.content.search;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.parameter.CompositeTitleSearchParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.client.parameter.TermsParameter;
import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;
import com.metabroadcast.sherlock.client.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.scoring.Weightings;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class SherlockSearchResolver implements SearchResolver {

    private static final ContentMapping CONTENT = IndexMapping.getContentMapping();

    private final SherlockSearcher searcher;
    private ContentResolver contentResolver;
    private LookupEntryStore lookupEntryStore;
    private final long timeout;

    public SherlockSearchResolver(
            SherlockSearcher searcher,
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
        CompositeTitleSearchParameter.Builder titleSearchParameter = CompositeTitleSearchParameter
                .forContentTitle(owlQuery.getTerm());

        if (owlQuery.getTitleWeighting() != 0.0f) {
            titleSearchParameter.withRelativeDefaultBoosts(owlQuery.getTitleWeighting());
        }

        SearchQuery.Builder searchQueryBuilder = SearchQuery.builder()
                .addSearcher(titleSearchParameter.build());

        //TODO priority channel weighting not currently supported since it would require calculating the channel
        // ids in this class before querying Sherlock. Since we do not likely need this we have left it out for now.
        if (owlQuery.getBroadcastWeighting() != 0.0f || owlQuery.getCatchupWeighting() != 0.0f) {
            QueryWeighting.Builder queryWeighting = QueryWeighting.builder();

            if (owlQuery.getBroadcastWeighting() != 0.0f) {
                queryWeighting.withWeighting(Weightings.broadcastCount(owlQuery.getBroadcastWeighting()));
            }

            if (owlQuery.getCatchupWeighting() != 0.0f) {
                queryWeighting.withWeighting(Weightings.availability(owlQuery.getCatchupWeighting()));
            }

            // owlQuery.getPriorityChannelWeighting()

            searchQueryBuilder.withQueryWeighting(queryWeighting.build());
        }

        if (owlQuery.getSelection() != null) {
            searchQueryBuilder
                    .withLimit(owlQuery.getSelection().getLimit())
                    .withOffset(owlQuery.getSelection().getOffset());
        }

        if (owlQuery.getIncludedSpecializations() != null) {
            for (Specialization specialization : owlQuery.getIncludedSpecializations()) {
                searchQueryBuilder.addFilter(
                        TermParameter.of(CONTENT.getSpecialization(), specialization.toString()));
            }
        }

        if (owlQuery.getIncludedPublishers() != null) {
            Set<String> sources = owlQuery.getIncludedPublishers().stream()
                    .map(Publisher::key)
                    .collect(MoreCollectors.toImmutableSet());
            searchQueryBuilder.addFilter(
                    TermsParameter.of(CONTENT.getSource().getKey(), sources));
            searchQueryBuilder.withIndex(SherlockIndex.CONTENT, sources);
        } else {
            searchQueryBuilder.withIndex(SherlockIndex.CONTENT);
        }

        if (owlQuery.type() != null) {
            searchQueryBuilder.addFilter(TermParameter.of(CONTENT.getType(), owlQuery.type()));
        }

        if (owlQuery.currentBroadcastsOnly() != null) {
            searchQueryBuilder.addFilter(
                    RangeParameter.from(
                            CONTENT.getBroadcasts().getTransmissionStartTime(),
                            // matches what was generally being done in Owl search indexing
                            Instant.now().minus(8, ChronoUnit.DAYS)
                    )
            );
        }

        if (owlQuery.topLevelOnly() != null) {
            searchQueryBuilder.addFilter(
                    TermParameter.of(CONTENT.getTopLevel(), owlQuery.topLevelOnly()));
        }

        return search(searchQueryBuilder.build());
    }

    public List<Identified> search(SearchQuery query) {
        try {
            IdSearchQueryResponse results = searcher.searchForIds(query)
                    .get(timeout, TimeUnit.MILLISECONDS);
            return resolveIds(results.getIds());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private List<Identified> resolveIds(List<Long> ids) {
        Set<LookupEntry> potentialCandidatesSet = ids.stream()
                .map(this::getLookupEntries) // from atlas owl
                .filter(Objects::nonNull)
                .flatMap(MoreStreams::stream)
                .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolved = contentResolver.findByCanonicalUris(potentialCandidatesSet.stream()
                .map(LookupEntry::uri)
                .collect(MoreCollectors.toImmutableSet()));

        return resolved.getAllResolvedResults();
    }

    private Iterable<LookupEntry> getLookupEntries(Long id) {
        return lookupEntryStore.entriesForIds(ImmutableSet.of(id));
    }

    public void setContentResolver(ContentResolver contentResolver) {
        this.contentResolver = contentResolver;
    }

    public void setLookupEntryStore(LookupEntryStore lookupEntryStore) {
        this.lookupEntryStore = lookupEntryStore;
    }
}
