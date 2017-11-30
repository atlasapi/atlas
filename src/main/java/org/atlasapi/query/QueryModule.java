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

import org.atlasapi.equiv.AllFromPublishersEquivalentContentResolver;
import org.atlasapi.equiv.EquivModule;
import org.atlasapi.equiv.query.MergeOnOutputQueryExecutor;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.cassandra.CassandraKnownTypeContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.query.content.ApplicationConfigurationQueryExecutor;
import org.atlasapi.query.content.CurieResolvingQueryExecutor;
import org.atlasapi.query.content.FilterActivelyPublishedOnlyQueryExecutor;
import org.atlasapi.query.content.FilterScheduleOnlyQueryExecutor;
import org.atlasapi.query.content.LookupResolvingQueryExecutor;
import org.atlasapi.query.content.UriFetchingQueryExecutor;
import org.atlasapi.query.uri.canonical.CanonicalisingFetcher;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.ImmutableSet;
import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import static org.atlasapi.media.entity.Publisher.FACEBOOK;

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

	@Bean @Primary KnownTypeQueryExecutor queryExecutor() {
	    
        MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(mongo.collection("lookup"), 
                new NoLoggingPersistenceAuditLog(), readPreference);
	    KnownTypeContentResolver mongoContentResolver = new MongoContentResolver(mongo, lookupStore);
        KnownTypeContentResolver cassandraContentResolver = new CassandraKnownTypeContentResolver(cassandra);

		DefaultEquivalentContentResolver defaultEquivalentContentResolver =
				new DefaultEquivalentContentResolver(
						mongoContentResolver,
						lookupStore
				);

		KnownTypeQueryExecutor queryExecutor =
				new LookupResolvingQueryExecutor(
						cassandraContentResolver,
						defaultEquivalentContentResolver,
						lookupStore,
						cassandraEnabled
				);

		queryExecutor = new UriFetchingQueryExecutor(localOrRemoteFetcher, queryExecutor, equivUpdater, ImmutableSet.of(FACEBOOK));
	    queryExecutor = new CurieResolvingQueryExecutor(queryExecutor);
	    queryExecutor = new FilterActivelyPublishedOnlyQueryExecutor(queryExecutor);
	    queryExecutor = new MergeOnOutputQueryExecutor(queryExecutor);
	    queryExecutor = new FilterScheduleOnlyQueryExecutor(queryExecutor);
	    
	    return Boolean.parseBoolean(applicationsEnabled) ? new ApplicationConfigurationQueryExecutor(queryExecutor) : queryExecutor;
	}

	// This is similar to the above, but does not use MergeOnOutput, because we want to equivalate
	// to single pieces of content, and not on merged mashes of content.
	@Bean @Qualifier("EquivalenceQueryExecutor") KnownTypeQueryExecutor equivalenceQueryExecutor() {
		MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(mongo.collection("lookup"),
				new NoLoggingPersistenceAuditLog(), readPreference);
		KnownTypeContentResolver mongoContentResolver = new MongoContentResolver(mongo, lookupStore);
		KnownTypeContentResolver cassandraContentResolver = new CassandraKnownTypeContentResolver(cassandra);

		DefaultEquivalentContentResolver defaultEquivalentContentResolver =
				new DefaultEquivalentContentResolver(
						mongoContentResolver,
						lookupStore
				);

		KnownTypeQueryExecutor queryExecutor =
				new LookupResolvingQueryExecutor(
						cassandraContentResolver,
						defaultEquivalentContentResolver,
						lookupStore,
						cassandraEnabled
				);

		queryExecutor = new UriFetchingQueryExecutor(localOrRemoteFetcher, queryExecutor, equivUpdater, ImmutableSet.of(FACEBOOK));
		queryExecutor = new CurieResolvingQueryExecutor(queryExecutor);
		queryExecutor = new FilterActivelyPublishedOnlyQueryExecutor(queryExecutor);
		queryExecutor = new FilterScheduleOnlyQueryExecutor(queryExecutor);

		return Boolean.parseBoolean(applicationsEnabled) ? new ApplicationConfigurationQueryExecutor(queryExecutor) : queryExecutor;
	}

	// This is similar to the @primary executor, but the EquivalentContentResolver it uses
	// allows for multiple equivs from the same publisher. This is written so that amazon content
	// can be merged on output.
	@Bean @Qualifier("YouviewQueryExecutor") KnownTypeQueryExecutor youviewQueryExecutor() {

		MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(mongo.collection("lookup"),
				new NoLoggingPersistenceAuditLog(), readPreference);
		KnownTypeContentResolver mongoContentResolver = new MongoContentResolver(mongo, lookupStore);
		KnownTypeContentResolver cassandraContentResolver = new CassandraKnownTypeContentResolver(cassandra);

		AllFromPublishersEquivalentContentResolver allFromPublishersEquivalentContentResolver =
				new AllFromPublishersEquivalentContentResolver(
						mongoContentResolver,
						lookupStore
				);

		KnownTypeQueryExecutor queryExecutor =
				new LookupResolvingQueryExecutor(
						cassandraContentResolver,
						allFromPublishersEquivalentContentResolver,
						lookupStore,
						cassandraEnabled
				);

		queryExecutor = new UriFetchingQueryExecutor(localOrRemoteFetcher, queryExecutor, equivUpdater, ImmutableSet.of(FACEBOOK));
		queryExecutor = new CurieResolvingQueryExecutor(queryExecutor);
		queryExecutor = new FilterActivelyPublishedOnlyQueryExecutor(queryExecutor);
		queryExecutor = new MergeOnOutputQueryExecutor(queryExecutor);
		queryExecutor = new FilterScheduleOnlyQueryExecutor(queryExecutor);

		return Boolean.parseBoolean(applicationsEnabled) ? new ApplicationConfigurationQueryExecutor(queryExecutor) : queryExecutor;
	}

}
