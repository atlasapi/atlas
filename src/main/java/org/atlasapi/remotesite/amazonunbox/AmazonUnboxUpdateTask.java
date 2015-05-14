package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class AmazonUnboxUpdateTask extends ScheduledTask {

    private final Logger log = LoggerFactory.getLogger(AmazonUnboxUpdateTask.class);
    
    private final AmazonUnboxItemProcessor itemPreProcessor;
    private final AmazonUnboxItemProcessor itemProcessor;
    private final AmazonUnboxHttpFeedSupplier feedSupplier;
    
    public AmazonUnboxUpdateTask(AmazonUnboxItemProcessor preHandler, 
                AmazonUnboxItemProcessor handler,
                AmazonUnboxHttpFeedSupplier feedSupplier) {
        this.itemPreProcessor = checkNotNull(preHandler);
        this.itemProcessor = checkNotNull(handler);
        this.feedSupplier = checkNotNull(feedSupplier);
    }

    @Override
    protected void runTask() {
        try  {
            
            AmazonUnboxProcessor<UpdateProgress> processor = processor(itemPreProcessor);
            
            ImmutableList<AmazonUnboxItem> items = feedSupplier.get();
            
            for (AmazonUnboxItem item : items) {
                processor.process(item);
            }
            
            itemPreProcessor.finish();
            
            reportStatus("Preprocessor: " + processor.getResult().toString());
            
            processor = processor(itemProcessor);
            
            for (AmazonUnboxItem item : items) {
                processor.process(item);
            }
            
            itemProcessor.finish();            
            reportStatus(processor.getResult().toString());
            
        } catch (Exception e) {
            reportStatus(e.getMessage());
            Throwables.propagate(e);
        }
    }

    private AmazonUnboxProcessor<UpdateProgress> processor(final AmazonUnboxItemProcessor handler) {
        return new AmazonUnboxProcessor<UpdateProgress>() {

            UpdateProgress progress = UpdateProgress.START;

            @Override
            public boolean process(AmazonUnboxItem aUItem) {
                try {
                    handler.process(aUItem);
                    progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.error("Error processing: " + aUItem.toString(), e);
                    progress.reduce(UpdateProgress.FAILURE);
                }
                return shouldContinue();
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }
}
