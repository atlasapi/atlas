package org.atlasapi.deer.elasticsearch;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentTitleSearcher {

    ListenableFuture<SearchResults> search(SearchQuery query);

}
