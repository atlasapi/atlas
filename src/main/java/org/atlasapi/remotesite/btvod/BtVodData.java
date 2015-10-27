package org.atlasapi.remotesite.btvod;

import java.io.IOException;
import java.util.Iterator;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Strings;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodData {
    
    private static final Logger log = LoggerFactory.getLogger(BtVodData.class);
    
    private static final String TEST_DATA_TITLE_PREFIX = "ZQX";
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
                log.debug("Processing entry {}", entry.getGuid());
                processor.process(entry);
            } else {
                log.debug("Filtering entry {}", entry.getGuid());
            }
        }
        return processor.getResult();
    }
    
    private boolean shouldFilter(BtVodEntry entry) {
        return !Strings.isNullOrEmpty(entry.getTitle()) &&
                entry.getTitle().startsWith(TEST_DATA_TITLE_PREFIX);
    }
    
}
