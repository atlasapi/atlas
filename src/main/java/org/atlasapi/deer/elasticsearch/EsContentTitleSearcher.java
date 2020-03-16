package org.atlasapi.deer.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.FilterBuilders.typeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;

public class EsContentTitleSearcher implements ContentTitleSearcher {

    private final Client index;

    public EsContentTitleSearcher(Client index) {
        this.index = index;
    }

    @Override
    public final ListenableFuture<SearchResults> search(SearchQuery search) {
        QueryBuilder titleQuery = null;
        QueryBuilder availabilityQuery = null;
        QueryBuilder broadcastQuery = null;
        QueryBuilder contentQuery = null;

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(search.getTerm()),
                "query term null or empty"
        );

        titleQuery = TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting());

        List<TermsFilterBuilder> filters = new LinkedList<TermsFilterBuilder>();
        if (search.getIncludedPublishers() != null && !search.getIncludedPublishers().isEmpty()) {
            filters.add(FiltersBuilder.buildForPublishers(
                    EsContent.SOURCE,
                    search.getIncludedPublishers()
            ));
        }
        if (search.getIncludedSpecializations() != null
                && !search.getIncludedSpecializations().isEmpty()) {
            filters.add(FiltersBuilder.buildForSpecializations(search.getIncludedSpecializations()));
        }
        if (!filters.isEmpty()) {
            titleQuery = QueryBuilders.filteredQuery(
                    titleQuery,
                    FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]))
            );
        }

        if (search.getBroadcastWeighting() != 0.0f) {
            broadcastQuery = BroadcastQueryBuilder.build(
                    titleQuery,
                    1f
            );
        } else {
            broadcastQuery = titleQuery;
        }

        if (search.getCatchupWeighting() != 0.0f) {
            availabilityQuery = AvailabilityQueryBuilder.build(
                    new Date(),
                    search.getCatchupWeighting()
            );
        }

        if (availabilityQuery != null) {
            contentQuery = QueryBuilders.boolQuery().must(broadcastQuery).should(availabilityQuery);
        } else {
            contentQuery = broadcastQuery;
        }

        QueryBuilder finalQuery = QueryBuilders.boolQuery()
                .should(
                        filteredQuery(
                                contentQuery,
                                andFilter(
                                        typeFilter(EsContent.TOP_LEVEL_ITEM),
                                        termFilter(EsContent.HAS_CHILDREN, Boolean.FALSE)
                                )
                        )
                )
                .should(
                        hasChildQuery(EsContent.CHILD_ITEM, contentQuery)
                                .scoreType("sum")
                );

        final SettableFuture<SearchResults> result = SettableFuture.create();
        index.prepareSearch(EsSchema.CONTENT_INDEX)
                .setQuery(finalQuery)
                .addField(EsContent.ID)
                .addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
                .setFrom(search.getSelection().getOffset())
                .setSize(search.getSelection().limitOrDefaultValue(10))
                .execute(new SearchResponseListener(result));
        return result;
    }

    private static class SearchResponseListener implements ActionListener<SearchResponse> {

        private final SettableFuture<SearchResults> result;

        public SearchResponseListener(SettableFuture<SearchResults> result) {
            this.result = result;
        }

        @Override
        public void onResponse(SearchResponse response) {
            Iterable<Long> uris = Iterables.transform(response.getHits(), new SearchHitToId());
            result.set(SearchResults.from(uris));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    private static class SearchHitToId implements Function<SearchHit, Long> {

        @Override
        public Long apply(SearchHit input) {
            Number idNumber = input.field(EsContent.ID).value();
            return idNumber.longValue();
        }
    }
}
