package org.atlasapi.equiv.update.tasks;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.metabroadcast.common.persistence.MongoTestHelper;

import junit.framework.TestCase;

public class
MongoScheduleTaskProgressStoreTest extends TestCase {

    private String taskName = "iamatask";
    private final MongoScheduleTaskProgressStore store = new MongoScheduleTaskProgressStore(MongoTestHelper.anEmptyTestDatabase());

    public void testSavingProgress() {

        ContentListingProgress progress = new ContentListingProgress(ContentCategory.CHILD_ITEM, Publisher.BBC, "one");
        
        store.storeProgress(taskName, progress);
        
        ContentListingProgress restored = store.progressForTask(taskName);
        
        assertEquals(progress.getUri(), restored.getUri());
        assertEquals(progress.getCategory(), restored.getCategory());
        assertEquals(progress.getPublisher(), restored.getPublisher());
        
        progress = new ContentListingProgress(null, null, null);
        
        store.storeProgress(taskName, progress);
        
        restored = store.progressForTask(taskName);
        
        assertNull(restored.getUri());
        assertNull(restored.getCategory());
        assertNull(restored.getPublisher());
        
    }
    
}
