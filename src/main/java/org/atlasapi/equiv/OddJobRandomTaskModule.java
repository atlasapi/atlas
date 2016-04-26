package org.atlasapi.equiv;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.mongodb.ReadPreference;
import org.atlasapi.equiv.update.tasks.ScheduleTaskProgressStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class OddJobRandomTaskModule {

    private @Autowired ContentLister lister;
    private @Autowired ContentResolver resolver;
    private @Autowired DatabasedMongo mongo;
    private @Autowired ScheduleTaskProgressStore progressStore;
    private @Autowired SimpleScheduler scheduler;

    @PostConstruct
    public void setup() {
//        scheduler.schedule(childRefUpdateTask(), RepetitionRules.NEVER);
//        scheduler.schedule(personRefUpdateTask(), RepetitionRules.NEVER);
//        scheduler.schedule(personLookupPopulationTask(), RepetitionRules.NEVER);
//        scheduler.schedule(lookupRefUpdateTask(), RepetitionRules.NEVER);
//        scheduler.schedule(tveChildRefUpdateTask().withName("TVE ChildRef update"), RepetitionRules.NEVER);
    }
    
    @Bean
    public ChildRefUpdateController childRefUpdateTaskController() {
        return new ChildRefUpdateController(childRefUpdateTask(), resolver);
    }
    
    @Bean
    public ChildRefUpdateTask tveChildRefUpdateTask() {
        return new ChildRefUpdateTask(lister, resolver, mongo, progressStore)
            .forPublishers(Publisher.BT_TVE_VOD, Publisher.BT_TVE_VOD_VOLD_CONFIG_1);
    }
    
    @Bean
    public ChildRefUpdateTask childRefUpdateTask() {
        return new ChildRefUpdateTask(lister, resolver, mongo, progressStore)
            .forPublishers(Publisher.all().toArray(new Publisher[]{}));
    }
    
    @Bean
    public PersonRefUpdateTask personRefUpdateTask() {
        return new PersonRefUpdateTask(lister, mongo, progressStore)
            .forPublishers(Publisher.RADIO_TIMES, Publisher.BBC, Publisher.PA);
    }
    
    @Bean
    public PersonLookupPopulationTask personLookupPopulationTask() {
        return new PersonLookupPopulationTask(mongo.collection("people"), 
                new MongoLookupEntryStore(mongo.collection("peopleLookup"), 
                        new NoLoggingPersistenceAuditLog(),
                        ReadPreference.primary()));
    }
    
    @Bean
    public LookupRefUpdateTask lookupRefUpdateTask() {
        return new LookupRefUpdateTask(mongo.collection("lookup"),
                mongo.collection("scheduling"));
    }
}
