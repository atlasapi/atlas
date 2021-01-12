package org.atlasapi.query.content.search;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;
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
    private final NumberToShortStringCodec idCodec;

    public SherlockSearchResolver(
            SherlockSearcher searcher,
            long timeout,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore,
            NumberToShortStringCodec idCodec
    ) {
        this.searcher = checkNotNull(searcher);
        this.timeout = timeout;
        this.contentResolver = contentResolver;
        this.lookupEntryStore = lookupEntryStore;
        this.idCodec = idCodec;
    }

    @Override
    public List<Identified> search(
            org.atlasapi.search.model.SearchQuery owlQuery,
            Application application
    ) {
        SearchParameter titleSearchParameter = SearchParameter.builder()
                .withMapping(CONTENT.getTitle())
                .withValue(owlQuery.getTerm())
                //.withBoost(owlQuery.getTitleWeighting())
                .build();

        // TODO Specific weightings are not yet supported in Sherlock
//        QueryWeighting queryWeighting = QueryWeighting.builder()
//                .withWeighting(Weightings.broadcastCount(owlQuery.getBroadcastWeighting()))
//                // .withWeighting(owlQuery.getCatchupWeighting())
//                // .withWeighting(owlQuery.getPriorityChannelWeighting())
//                .build();

        SearchQuery.Builder searchQueryBuilder = SearchQuery.builder()
                .addSearcher(titleSearchParameter);
                //.withQueryWeighting(queryWeighting);

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
            for (Publisher publisher : owlQuery.getIncludedPublishers()) {
                searchQueryBuilder.addFilter(
                        TermParameter.of(CONTENT.getSource().getKey(), publisher.key()));
            }
            searchQueryBuilder.withIndex(
                    SherlockIndex.CONTENT,
                    owlQuery.getIncludedPublishers().stream()
                            .map(Publisher::key)
                            .collect(MoreCollectors.toImmutableSet())
            );
        } else {
            searchQueryBuilder.withIndex(SherlockIndex.CONTENT);
        }

        if (owlQuery.type() != null) {
            searchQueryBuilder.addFilter(TermParameter.of(CONTENT.getType(), owlQuery.type()));
        }

        if (owlQuery.currentBroadcastsOnly() != null) {
            // Not implemented because Sherlock does not support this either
//            searchQueryBuilder.addFilter()
//            deerSearchQuery.withCurrentBroadcastsOnly(owlQuery.currentBroadcastsOnly());
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
