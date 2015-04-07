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

package org.atlasapi;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;

import org.atlasapi.system.JettyHealthProbe;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.webapp.properties.ContextConfigurer;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

@Configuration
public class AtlasModule {
    
	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String dbName = Configurer.get("mongo.dbName").get();
	private static final int mongoMaxConnections = Configurer.get("mongo.maxConnections").toInt();
	private final String mongoTag = Strings.emptyToNull(Configurer.get("mongo.db.tag").get());
	private final String mongoFallbackTag = Strings.emptyToNull(Configurer.get("mongo.db.tag.fallback").get());
	private final Parameter processingConfig = Configurer.get("processing.config");
	private final Parameter processingWriteConcern = Configurer.get("processing.mongo.writeConcern");
	private final MongoSecondaryReadPreferenceBuilder secondaryReadPreferenceBuilder = new MongoSecondaryReadPreferenceBuilder();

	public @Bean DatabasedMongo databasedMongo() {
	    return new DatabasedMongo(mongo(), dbName);
	}

    public @Bean Mongo mongo() {
        MongoOptions options = new MongoOptions();
        options.setConnectionsPerHost(mongoMaxConnections);
        Mongo mongo = new Mongo(mongoHosts(), options);
        mongo.setReadPreference(readPreference());
        if(isProcessing() 
                && processingWriteConcern != null 
                && !Strings.isNullOrEmpty(processingWriteConcern.get())) {
                
            WriteConcern writeConcern = WriteConcern.valueOf(processingWriteConcern.get());
            if (writeConcern == null) {
                throw new IllegalArgumentException("Could not parse write concern: " + 
                                processingWriteConcern.get());
            }
            mongo.setWriteConcern(writeConcern);
        }
        return mongo;
    }

    public @Bean ReadPreference readPreference() {
        boolean requirePrimary = isProcessing();
        if (requirePrimary) {
            return ReadPreference.primary();
        }
        
        ImmutableList.Builder<String> tags = ImmutableList.builder();
        if (mongoTag != null) {
            tags.add(mongoTag);
        }
        
        if (mongoFallbackTag != null) {
            tags.add(mongoFallbackTag);
        }
        
        return secondaryReadPreferenceBuilder.fromProperties(tags.build());
    }

    private boolean isProcessing() {
        return processingConfig != null && processingConfig.toBoolean();
    }
    
    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(mongoHost), new Function<String, ServerAddress>() {

            @Override
            public ServerAddress apply(String input) {
                try {
                    return new ServerAddress(input, 27017);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }
    
    public @Bean HealthProbe jettyHealthProbe() {
        return new JettyHealthProbe();
    }

	public @Bean ContextConfigurer config() {
		ContextConfigurer c = new ContextConfigurer();
		c.init();
		return c;
	}
}
