package org.atlasapi.remotesite.btvod;

import java.io.IOException;
import java.util.Iterator;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodData {

    private BtMpxVodClient vodClient;
    
    public BtVodData(BtMpxVodClient vodClient) {
        this.vodClient = checkNotNull(vodClient);
    }
    
    public <T> T processData(BtVodDataProcessor<T> processor) throws IOException {
        Iterator<BtVodEntry> btMpxFeed = vodClient.getBtMpxFeed();
        while (btMpxFeed.hasNext()) {
            processor.process(btMpxFeed.next());
        }
        return processor.getResult();
    }
    
}
