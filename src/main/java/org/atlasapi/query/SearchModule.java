package org.atlasapi.query;

import javax.annotation.PostConstruct;

import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.query.content.fuzzy.RemoteFuzzySearcher;
import org.atlasapi.query.content.search.ContentResolvingSearcher;
import org.atlasapi.query.content.search.DummySearcher;
import org.atlasapi.query.content.search.DeerSearchResolver;
import org.atlasapi.search.ContentSearcher;

import com.metabroadcast.common.properties.Configurer;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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

    private final boolean esSsl = Configurer.get("elasticsearch.ssl").toBoolean();
    private final String esCluster = Configurer.get("elasticsearch.cluster").get();
    private final boolean esSniff = Configurer.get("elasticsearch.sniff").toBoolean();
    private final String esTimeout = Configurer.get("elasticsearch.timeout").get();
    private final String esSeeds = Configurer.get("elasticsearch.seeds").get();
    private final int port = Ints.saturatedCast(Configurer.get("elasticsearch.port").toLong());

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

        SearchResolver deerIdSearcher = equivDeerSearchResolver();
        if (deerIdSearcher instanceof DeerSearchResolver) {
            DeerSearchResolver resolver = (DeerSearchResolver) deerIdSearcher;
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
    @Qualifier("DeerSearchResolver")
    SearchResolver equivDeerSearchResolver() {
        TransportClient esClient = getTransportClient();
        ContentTitleSearcher contentSearcher = new EsContentTitleSearcher(esClient);
        return new DeerSearchResolver(
                contentSearcher,
                Long.parseLong(esTimeout),
                null,
                null
        );
    }

    private TransportClient getTransportClient() {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", esSniff)
                .put("cluster.name", esCluster)
                .put("shield.transport.ssl", esSsl)
                .build();

        TransportClient esClient = new TransportClient(settings);

        for (String host : esSeeds.split(",")) {
            esClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }

        return esClient;
    }
}
