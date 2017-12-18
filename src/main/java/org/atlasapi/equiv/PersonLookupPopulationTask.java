package org.atlasapi.equiv;

import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.media.entity.PersonTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PersonLookupPopulationTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(PersonLookupPopulationTask.class);
    
    private final DBCollection collection;
    private final MongoLookupEntryStore lookupEntryStore;
    private final PersonTranslator translator = new PersonTranslator();

    public PersonLookupPopulationTask(DBCollection collection,
            MongoLookupEntryStore mongoLookupEntryStore) {
        this.collection = collection;
        this.lookupEntryStore = mongoLookupEntryStore;
    }

    @Override
    protected void runTask() {
        DBCursor cursor = collection.find();
        UpdateProgress progress = UpdateProgress.START;
        for(DBObject dbo : cursor) {
            try {
                Person person = translator.fromDBObject(dbo, null);
                lookupEntryStore.store(LookupEntry.lookupEntryFrom(person));
                progress = progress.reduce(UpdateProgress.SUCCESS);
            } catch (Exception e) {
                log.error("Problem with a person", e);
                progress = progress.reduce(UpdateProgress.FAILURE);
            }
            reportStatus(progress.toString());
        }
    }
    
}
