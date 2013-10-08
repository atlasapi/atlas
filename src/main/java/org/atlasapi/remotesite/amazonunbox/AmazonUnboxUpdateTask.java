package org.atlasapi.remotesite.amazonunbox;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class AmazonUnboxUpdateTask extends ScheduledTask {

    private final Logger log = LoggerFactory.getLogger(AmazonUnboxUpdateTask.class);
    
    private final AmazonUnboxFileUpdater fileUpdater;
    private final AmazonUnboxFileStore store;
    private final AmazonUnboxItemProcessor preHandler;
    private final AmazonUnboxItemProcessor handler;
    
    public AmazonUnboxUpdateTask(AmazonUnboxFileUpdater fileUpdater, AmazonUnboxFileStore store, AmazonUnboxItemProcessor preHandler, AmazonUnboxItemProcessor handler) {
        this.fileUpdater = fileUpdater;
        this.store = store;
        this.preHandler = preHandler;
        this.handler = handler;
    }

    @Override
    protected void runTask() {
        try  {
            
            reportStatus("Updating File");
            fileUpdater.update();
            reportStatus("File updated");
            
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            
            AmazonUnboxProcessor<UpdateProgress> processor = processor(preHandler);
            
            AmazonUnboxContentHandler xmlHandler = new AmazonUnboxContentHandler(processor);
            
            saxParser.parse(store.getLatestData(), xmlHandler);
            reportStatus("Preprocessor: " + processor.getResult().toString());
            
            reportStatus("Pre-processing complete.");
            
            processor = processor(handler);
            xmlHandler = new AmazonUnboxContentHandler(processor);
            
            saxParser.parse(store.getLatestData(), xmlHandler);
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
