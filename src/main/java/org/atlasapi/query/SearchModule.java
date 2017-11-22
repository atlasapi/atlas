package org.atlasapi.query;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.query.content.fuzzy.RemoteFuzzySearcher;
import org.atlasapi.query.content.search.ContentResolvingSearcher;
import org.atlasapi.query.content.search.DummySearcher;
import org.atlasapi.search.ContentSearcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Strings;
import org.springframework.context.annotation.Primary;

@Configuration
public class SearchModule {

    private @Value("${atlas.search.host}") String searchHost;

    //this will use an executor that does not do merging.
    private @Autowired @Qualifier("EquivalenceQueryExecutor") KnownTypeQueryExecutor equivQueryExecutor;
    private @Autowired KnownTypeQueryExecutor queryExecutor;
    
    private @Autowired PeopleQueryResolver peopleQueryResolver;
    
    @PostConstruct
    public void setExecutor() {
        SearchResolver searchResolver = searchResolver();
        if (searchResolver instanceof ContentResolvingSearcher) {
            ContentResolvingSearcher resolver = (ContentResolvingSearcher) searchResolver;
            resolver.setExecutor(queryExecutor);
            resolver.setPeopleQueryResolver(peopleQueryResolver);
        }

        SearchResolver equivSearchResolver = equivSearchResolver();
        if (equivSearchResolver instanceof ContentResolvingSearcher) {
            ContentResolvingSearcher resolver = (ContentResolvingSearcher) searchResolver;
            resolver.setExecutor(equivQueryExecutor);
            resolver.setPeopleQueryResolver(peopleQueryResolver);
        }
    }
    
    @Bean
    @Primary
    SearchResolver searchResolver() {
        if (!Strings.isNullOrEmpty(searchHost)) {
            ContentSearcher titleSearcher = new RemoteFuzzySearcher(searchHost);
            return new ContentResolvingSearcher(titleSearcher, null, null);
        }

        return new DummySearcher();
    }

    @Bean
    @Qualifier("EquivalenceSearchResolver")
    SearchResolver equivSearchResolver() {
        if (!Strings.isNullOrEmpty(searchHost)) {
            ContentSearcher titleSearcher = new RemoteFuzzySearcher(searchHost);
            return new ContentResolvingSearcher(titleSearcher, null, null);
        }

        return new DummySearcher();
    }

}
