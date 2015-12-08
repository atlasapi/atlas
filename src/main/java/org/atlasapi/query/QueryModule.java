/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.query;

import static org.atlasapi.media.entity.Publisher.FACEBOOK;

import org.atlasapi.equiv.EquivModule;
import org.atlasapi.equiv.query.MergeOnOutputQueryExecutor;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.cassandra.CassandraKnownTypeContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoTopLevelItemsEntry;
import org.atlasapi.persistence.content.mongo.TopLevelItemsEntry;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.query.content.ApplicationConfigurationQueryExecutor;
import org.atlasapi.query.content.CurieResolvingQueryExecutor;
import org.atlasapi.query.content.EventResolvingQueryExecutor;
import org.atlasapi.query.content.FilterActivelyPublishedOnlyQueryExecutor;
import org.atlasapi.query.content.FilterScheduleOnlyQueryExecutor;
import org.atlasapi.query.content.LookupResolvingQueryExecutor;
import org.atlasapi.query.content.UriFetchingQueryExecutor;
import org.atlasapi.query.uri.canonical.CanonicalisingFetcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.ReadPreference;

@Configuration
@Import(EquivModule.class)
public class QueryModule {

	private @Autowired @Qualifier("remoteSiteContentResolver") CanonicalisingFetcher localOrRemoteFetcher;
	
	private @Autowired DatabasedMongo mongo;
	private @Autowired ReadPreference readPreference;
    private @Autowired CassandraContentStore cassandra;
    private @Autowired @Qualifier("contentUpdater") EquivalenceUpdater<Content> equivUpdater;
	
	private @Value("${applications.enabled}") String applicationsEnabled;
	private @Value("${atlas.search.host}") String searchHost;
	private @Value("${cassandra.enabled}") boolean cassandraEnabled;

	@Bean KnownTypeQueryExecutor queryExecutor() {
	    
        MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(mongo.collection("lookup"), 
                new NoLoggingPersistenceAuditLog(), readPreference);
	    KnownTypeContentResolver mongoContentResolver = new MongoContentResolver(mongo, lookupStore);
        KnownTypeContentResolver cassandraContentResolver = new CassandraKnownTypeContentResolver(cassandra);

		TopLevelItemsEntry itemsEntry = new MongoTopLevelItemsEntry(mongo.collection("topLevelItems"), readPreference);

		KnownTypeQueryExecutor queryExecutor = new EventResolvingQueryExecutor(cassandraContentResolver,
				mongoContentResolver, lookupStore, cassandraEnabled, itemsEntry);

		queryExecutor = new LookupResolvingQueryExecutor(queryExecutor);

		queryExecutor = new UriFetchingQueryExecutor(localOrRemoteFetcher, queryExecutor, equivUpdater, ImmutableSet.of(FACEBOOK));
		
	    queryExecutor = new CurieResolvingQueryExecutor(queryExecutor);
		
	    queryExecutor = new FilterActivelyPublishedOnlyQueryExecutor(queryExecutor);
	    queryExecutor = new MergeOnOutputQueryExecutor(queryExecutor);
	    queryExecutor = new FilterScheduleOnlyQueryExecutor(queryExecutor);
	    
	    return Boolean.parseBoolean(applicationsEnabled) ? new ApplicationConfigurationQueryExecutor(queryExecutor) : queryExecutor;
	}
//	
//	@Bean @Lazy SearchResolver searchResolver() {
//	    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + applicationsEnabled);
//	    if (! Strings.isNullOrEmpty(searchHost)) {
//	        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + searchHost);
//    	    ContentSearcher titleSearcher = new RemoteFuzzySearcher(searchHost);
//    	    return new ContentResolvingSearcher(titleSearcher, queryExecutor());
//	    }
//	    
//	    return new DummySearcher();
//	}
}
