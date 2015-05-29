package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.util.OldContentDeactivator;

import com.google.common.collect.Lists;


public class BtVodOldContentDeactivator implements BtVodContentListener {

    private final List<String> validContentUris = Lists.newArrayList();
    private final OldContentDeactivator oldContentDeactivator;
    private final Publisher publisher;
    private int threshold;
    
    public BtVodOldContentDeactivator(Publisher publisher, 
            OldContentDeactivator oldContentDeactivator, int threshold) {
        this.publisher = checkNotNull(publisher);
        this.oldContentDeactivator = checkNotNull(oldContentDeactivator);
        this.threshold = threshold;
    }
    
    public void start() {
        validContentUris.clear();
    }
    
    @Override
    public void onContent(Content content, BtVodEntry vodData) {
        validContentUris.add(content.getCanonicalUri());
    }
    
    public void finish() {
        oldContentDeactivator.deactivateOldContent(publisher, validContentUris, threshold);
    }

}
