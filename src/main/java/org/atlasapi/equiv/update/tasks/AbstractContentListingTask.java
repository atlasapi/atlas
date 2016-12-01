package org.atlasapi.equiv.update.tasks;

import java.util.Iterator;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

import com.metabroadcast.common.scheduling.ScheduledTask;

@Beta
public abstract class AbstractContentListingTask<C extends Content> extends ScheduledTask {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentLister contentLister;

    public AbstractContentListingTask(ContentLister contentLister) {
        this.contentLister = contentLister;
    }
    
    private Iterator<Content> getContentIterator(ContentListingCriteria criteria) {
        return contentLister.listContent(criteria);
    }
    
    @Override
    protected void runTask() {
        ContentListingProgress progress = getProgress();

        onStart(progress);
        
        Iterator<C> contents = filter(getContentIterator(listingCriteria(progress)));
        
        boolean proceed = true;
        C current = null;
        try {
            while(shouldContinue() && proceed && contents.hasNext()) {
                current = contents.next();
                proceed = handle(current);
            }
        } catch (Exception e) {
            log.error(getName(), e);
            onFinish(false, current);
        }
        onFinish(proceed && shouldContinue(), current);
    }

    protected void onStart(ContentListingProgress progress){};
    protected void onFinish(boolean finished, @Nullable Content lastProcessed){};

    protected abstract Iterator<C> filter(Iterator<Content> rawIterator);
    
    protected abstract boolean handle(C content);

    protected abstract ContentListingCriteria listingCriteria(ContentListingProgress progress);
    
    protected ContentListingProgress getProgress() {
        return ContentListingProgress.START;
    }
}
