package org.atlasapi.logging;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import org.atlasapi.logging.www.LogViewingController;
import org.atlasapi.persistence.logging.MongoLoggingAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.atlasapi.AtlasModule.OWL_DATABASED_MONGO;

@Configuration
public class AtlasLoggingModule {
	
	private @Autowired @Qualifier(OWL_DATABASED_MONGO) DatabasedMongo db;
	
	public @Bean MongoLoggingAdapter adapterLog() {
		return new MongoLoggingAdapter(db);
	}
	
	public @Bean LogViewingController logView() {
		return new LogViewingController(adapterLog());
	}
}
