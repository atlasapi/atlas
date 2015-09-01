package org.atlasapi.remotesite.btvod;

import java.io.IOException;
import java.util.Iterator;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodData {

    private static final CharSequence BACK_TO_BACKS_SUBGENRE = "Back to Back";
    private static final String FEED_NAME = "btv-prd-search";
    private BtMpxVodClient vodClient;
    private final String feedName;

    public BtVodData(BtMpxVodClient vodClient, String feedName) {
        this.vodClient = checkNotNull(vodClient);
        this.feedName = checkNotNull(feedName);
    }
    
    public <T> T processData(BtVodDataProcessor<T> processor) throws IOException {
        Iterator<BtVodEntry> btMpxFeed = vodClient.getFeed(feedName);
        while (btMpxFeed.hasNext()) {
            processor.process(btMpxFeed.next());
        }
        return processor.getResult();
    }
    
}
