package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import com.metabroadcast.columbus.telescope.api.Event;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
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
    
    public AmazonUnboxUpdateTask(
            AmazonUnboxItemProcessor preHandler,
            AmazonUnboxItemProcessor handler,
            AmazonUnboxHttpFeedSupplier feedSupplier
    ) {
        this.itemPreProcessor = checkNotNull(preHandler);
        this.itemProcessor = checkNotNull(handler);
        this.feedSupplier = checkNotNull(feedSupplier);
    }

    @Override
    protected void runTask() {
        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory
                .getInstance()
                .getTelescopeReporter(
                    OwlTelescopeReporters.AMAZON_UNBOX_UPDATE_TASK,
                    Event.Type.INGEST
        );
        telescope.startReporting();

        try  {
            
            AmazonUnboxProcessor<UpdateProgress> processor = processor(itemPreProcessor, telescope);

            ImmutableList<AmazonUnboxItem> items = feedSupplier.get();
            
            for (AmazonUnboxItem item : items) {
                processor.process(item);
            }
            
            itemPreProcessor.finish(telescope);
            
            reportStatus("Preprocessor: " + processor.getResult().toString());
            
            processor = processor(itemProcessor, telescope);
            
            for (AmazonUnboxItem item : items) {
                processor.process(item);
            }
            
            itemProcessor.finish(telescope);
            reportStatus(processor.getResult().toString());
            
        } catch (Exception e) {
            telescope.reportFailedEvent("An exception has prevented this task from " +
                    "completing properly (" + e.getMessage() + ")");
            reportStatus(e.getMessage());
            Throwables.propagate(e);
        }
        telescope.endReporting();
    }

    private AmazonUnboxProcessor<UpdateProgress> processor(
            final AmazonUnboxItemProcessor handler,
            OwlTelescopeReporter telescope) {
        return new AmazonUnboxProcessor<UpdateProgress>() {

            UpdateProgress progress = UpdateProgress.START;

            @Override
            public boolean process(AmazonUnboxItem aUItem) {
                try {
                    handler.process(aUItem);
                    progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    telescope.reportFailedEvent("Error processing: " +
                            aUItem.toString() + " (" + e.getMessage());
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
