package org.atlasapi.remotesite.btvod;

import java.io.IOException;
import java.util.Iterator;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;

import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodData {

    private static final CharSequence BACK_TO_BACKS_SUBGENRE = "Back to Back";
    private BtMpxVodClient vodClient;
    private final String feedName;

    public BtVodData(BtMpxVodClient vodClient, String feedName) {
        this.vodClient = checkNotNull(vodClient);
        this.feedName = checkNotNull(feedName);
    }
    
    public <T> T processData(BtVodDataProcessor<T> processor) throws IOException {
        Iterator<BtVodEntry> btMpxFeed = vodClient.getFeed(feedName);
        while (btMpxFeed.hasNext()) {
            BtVodEntry entry = btMpxFeed.next();
            if (!shouldFilter(entry)) {
                processor.process(entry);
            }
        }
        return processor.getResult();
    }
    
    private boolean shouldFilter(BtVodEntry entry) {
        if (entry.getProductScopes().isEmpty()) {
            return false;
        }
        BtVodProductScope scope = Iterables.getFirst(entry.getProductScopes(), null);
        // It's not possible to fashion an MPX filter to exclude all back-to-backs, so we
        // must filter here
        return scope.getProductMetadata() != null 
                && scope.getProductMetadata().getSubGenres() != null
                && scope.getProductMetadata().getSubGenres().contains(BACK_TO_BACKS_SUBGENRE);
       
    }
    
}
