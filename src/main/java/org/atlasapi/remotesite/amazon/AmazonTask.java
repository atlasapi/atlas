package org.atlasapi.remotesite.amazon;

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


public class AmazonTask extends ScheduledTask {

    private final Logger log = LoggerFactory.getLogger(AmazonTask.class);
    
    private final AmazonItemProcessor itemPreProcessor;
    private final AmazonItemProcessor itemProcessor;
    private final AmazonHttpFeedSupplier feedSupplier;
    
    public AmazonTask(
            AmazonItemProcessor preHandler,
            AmazonItemProcessor handler,
            AmazonHttpFeedSupplier feedSupplier
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
                    OwlTelescopeReporters.AMAZON_PRIME_VIDEO_UPDATE_TASK,
                    Event.Type.INGEST
        );
        telescope.startReporting();

        try  {
            itemPreProcessor.prepare(telescope);
            AmazonProcessor<UpdateProgress> processor = processor(itemPreProcessor, telescope);
            ImmutableList<AmazonItem> items = feedSupplier.get();
            for (AmazonItem item : items) {
                processor.process(item);
            }
            itemPreProcessor.finish();
            
            reportStatus("Preprocessor: " + processor.getResult().toString());

            itemProcessor.prepare(telescope);
            processor = processor(itemProcessor, telescope);
            for (AmazonItem item : items) {
                processor.process(item);
            }
            itemProcessor.finish();

            reportStatus(processor.getResult().toString());

            // Dont put this into a finally since we dont want to end reporting when something major
            // happens. This will help alert us.
            telescope.endReporting();
            
        } catch (Exception e) {
            telescope.reportFailedEvent("An exception has prevented this task from " +
                    "completing properly (" + e.getMessage() + ")");
            reportStatus(e.getMessage());
            telescope.endReporting();
            Throwables.propagate(e);
        }
        telescope.endReporting();
    }

    private AmazonProcessor<UpdateProgress> processor(
            final AmazonItemProcessor handler,
            OwlTelescopeReporter telescope) {
        return new AmazonProcessor<UpdateProgress>() {

            UpdateProgress progress = UpdateProgress.START;

            @Override
            public boolean process(AmazonItem amazonItem) {
                try {
                    handler.process(amazonItem);
                    progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    telescope.reportFailedEvent("Unable to process item. (" + e.getMessage() + ")", amazonItem);
                    log.error("Error processing: " + amazonItem.toString(), e);
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
