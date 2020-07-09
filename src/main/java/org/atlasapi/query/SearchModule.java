package org.atlasapi.query;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.query.content.fuzzy.RemoteFuzzySearcher;
import org.atlasapi.query.content.search.ContentResolvingSearcher;
import org.atlasapi.query.content.search.SherlockSearchResolver;
import org.atlasapi.query.content.search.DummySearcher;
import org.atlasapi.search.ContentSearcher;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.client.ElasticSearchProcessor;
import com.metabroadcast.sherlock.common.config.ElasticSearchConfig;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class SearchModule {

    private @Value("${atlas.search.host}") String searchHost;

    //this will use an executor that does not do merging.
    private @Autowired @Qualifier("EquivalenceQueryExecutor") KnownTypeQueryExecutor equivQueryExecutor;
    private @Autowired KnownTypeQueryExecutor queryExecutor;
    
    private @Autowired PeopleQueryResolver peopleQueryResolver;

    private @Autowired ContentResolver contentResolver;
    private @Autowired LookupEntryStore lookupEntryStore;

    private final String sherlockScheme = Configurer.get("sherlock.scheme").get();
    private final String sherlockHostname = Configurer.get("sherlock.hostname").get();
    private final int sherlockPort = Ints.saturatedCast(Configurer.get("sherlock.port").toLong());
    private final int sherlockTimeout = Ints.saturatedCast(Configurer.get("sherlock.timeout").toLong());

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
            ContentResolvingSearcher resolver = (ContentResolvingSearcher) equivSearchResolver;
            resolver.setExecutor(equivQueryExecutor);
            resolver.setPeopleQueryResolver(peopleQueryResolver);
        }

        SearchResolver sherlockIdSearcher = sherlockSearchResolver();
        if (sherlockIdSearcher instanceof SherlockSearchResolver) {
            SherlockSearchResolver resolver = (SherlockSearchResolver) sherlockIdSearcher;
            resolver.setContentResolver(contentResolver);
            resolver.setLookupEntryStore(lookupEntryStore);
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

    @Bean
    @Qualifier("SherlockSearchResolver")
    SearchResolver sherlockSearchResolver() {
        return new SherlockSearchResolver(
                contentSearcher(),
                sherlockTimeout,
                null,
                null,
                SubstitutionTableNumberCodec.lowerCaseOnly()
        );
    }

    @Bean
    public ElasticSearchConfig sherlockElasticSearchConfig() {
        return ElasticSearchConfig.builder()
                .withScheme(sherlockScheme)
                .withHostname(sherlockHostname)
                .withPort(sherlockPort)
                .build();
    }

    @Bean
    @Primary
    public com.metabroadcast.sherlock.client.search.ContentSearcher contentSearcher() {
        return com.metabroadcast.sherlock.client.search.ContentSearcher.builder()
                .withElasticSearchProcessor(
                        new ElasticSearchProcessor(
                                sherlockElasticSearchConfig().getElasticSearchClient()
                        )
                )
                .withIndex(SherlockIndex.CONTENT)
                .build();
    }
}
