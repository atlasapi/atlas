package org.atlasapi.equiv.update.tasks;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoUpdateBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoScheduleTaskProgressStore implements ScheduleTaskProgressStore {
    
    private static final String SCHEDULING_COLLECTION_NAME = "scheduling";
    
    private static final String START = "start";
    private static final String PUBLISHER = "publisher";
    private static final String LAST_ID = "lastId";
    private static final String CATEGORY = "category";
    
    private final DBCollection collection;

    public MongoScheduleTaskProgressStore(DatabasedMongo mongo) {
        collection = mongo.collection(SCHEDULING_COLLECTION_NAME);
    }

    @Override
    public ContentListingProgress progressForTask(String taskName) {
        DBObject existing = collection.findOne(taskName);
        return existing != null ? fromDbo(existing) : ContentListingProgress.START;
    }

    private ContentListingProgress fromDbo(DBObject progress) {
        String lastId = TranslatorUtils.toString(progress, LAST_ID);
        
        if(START.equals(lastId)) {
            return ContentListingProgress.START;
        }
        
        String tableName = TranslatorUtils.toString(progress, CATEGORY);
        ContentCategory category = tableName == null ? null : ContentCategory.valueOf(tableName);
        
        String pubKey = TranslatorUtils.toString(progress, PUBLISHER);
        Publisher publisher = pubKey == null ? null : Publisher.fromKey(pubKey).valueOrNull();
        
        return new ContentListingProgress(category, publisher, lastId);
    }

    @Override
    public void storeProgress(String taskName, ContentListingProgress progress) {
        MongoUpdateBuilder update = update().setField(LAST_ID, progress.getUri() == null ? START : progress.getUri());
        
        if(progress.getCategory() != null) {
            update.setField(CATEGORY, progress.getCategory().toString());
        } else {
            update.unsetField(CATEGORY);
        }
        
        if(progress.getPublisher() != null) {
            update.setField(PUBLISHER, progress.getPublisher().key());
        } else {
            update.unsetField(PUBLISHER);
        }
        
        collection.update(
                where().fieldEquals(ID, taskName).build(),
                update.build(),
                UPSERT,
                SINGLE
        );
    }

}
