package org.atlasapi.remotesite.rte;

import static com.google.common.base.Preconditions.checkNotNull;
import nu.xom.Document;

import com.google.common.base.Supplier;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class RteFeedUpdater extends ScheduledTask {

    private final Supplier<Document> feedSupplier;
    private final RteFeedProcessor feedProcessor;
    
    public RteFeedUpdater(Supplier<Document> feedSupplier, RteFeedProcessor feedProcessor) {
        this.feedSupplier = checkNotNull(feedSupplier);
        this.feedProcessor = checkNotNull(feedProcessor);
    }
    
    @Override
    protected void runTask() {
        feedProcessor.process(feedSupplier.get(), this.reporter());
    }
    
}
