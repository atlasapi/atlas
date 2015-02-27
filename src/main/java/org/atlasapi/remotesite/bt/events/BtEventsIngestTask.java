package org.atlasapi.remotesite.bt.events;

import org.atlasapi.remotesite.bt.events.model.BtEvent;
import org.atlasapi.remotesite.events.EventsIngestTask;
import org.slf4j.LoggerFactory;


public class BtEventsIngestTask extends EventsIngestTask<BtSportType, BtTeam, BtEvent> {
    
    
    public BtEventsIngestTask(BtEventsFetcher fetcher, BtEventsDataHandler dataHandler) {
        super(LoggerFactory.getLogger(BtEventsIngestTask.class), fetcher, dataHandler);
    }
}
