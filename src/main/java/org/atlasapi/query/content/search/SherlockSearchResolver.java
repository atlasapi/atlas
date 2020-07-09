package org.atlasapi.query.content.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.Term;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SearchQueryResponse;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.client.search.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.search.scoring.Weightings;
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

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class SherlockSearchResolver implements SearchResolver {

    private static final ContentMapping CONTENT = IndexMapping.getContent();

    private final ContentSearcher searcher;
    private ContentResolver contentResolver;
    private LookupEntryStore lookupEntryStore;
    private final long timeout;
    private final NumberToShortStringCodec idCodec;

    public SherlockSearchResolver(
            ContentSearcher searcher,
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

        // TODO These specific weighting functionalities do not currently exist in Sherlock, so
        // commented out all boosts/weightings for the time being

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
        }

        if (owlQuery.type() != null) {
            searchQueryBuilder.addFilter(TermParameter.of(CONTENT.getType(), owlQuery.type()));
        }

        if (owlQuery.currentBroadcastsOnly() != null) {
            // TODO is this for future broadcasts, or broadcasts that are currently being broadcast?
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
            SearchQueryResponse results = searcher.searchForIds(query)
                    .get(timeout, TimeUnit.MILLISECONDS);
            List<Long> ids = decodeIds(results.getIds());
            return resolveIds(ids);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private List<Long> decodeIds(Iterable<String> input) {
        if (input == null) {
            return ImmutableList.of();
        } else {
            return StreamSupport.stream(input.spliterator(), false)
                    .map(idCodec::decode)
                    .map(BigInteger::longValue)
                    .collect(Collectors.toList());
        }
    }

    private List<Identified> resolveIds(List<Long> ids) {
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
