package org.atlasapi.remotesite.lovefilm;

import static org.atlasapi.remotesite.lovefilm.LoveFilmCsvColumn.SKU;

import org.atlasapi.feeds.utils.UpdateProgress;
import org.atlasapi.remotesite.lovefilm.LoveFilmData.LoveFilmDataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class LoveFilmCsvUpdateTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(LoveFilmCsvUpdateTask.class);
    
    private final LoveFilmFileUpdater updater;
    private final LoveFilmDataRowHandler dataHandler;
    private final LoveFilmFileStore store;

    public LoveFilmCsvUpdateTask(LoveFilmFileUpdater updater, LoveFilmFileStore store, LoveFilmDataRowHandler dataHandler) {
        this.updater = updater;
        this.store = store;
        this.dataHandler = dataHandler;
    }
    
    @Override
    protected void runTask() {
        try {
//            LoveFilmData latestData = dataSupplier.getLatestData();
            updater.update();
            LoveFilmData latestData = store.fetchLatestData();
            
            dataHandler.prepare();
            UpdateProgress progress = latestData.processData(processor());
            dataHandler.finish();
            
            reportStatus(progress.toString());
            
        } catch (Exception e) {
            reportStatus(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    protected LoveFilmDataProcessor<UpdateProgress> processor() {
        return new LoveFilmDataProcessor<UpdateProgress>() {
            
            UpdateProgress progress = UpdateProgress.START;
            
            @Override
            public boolean process(LoveFilmDataRow row) {
                try {
                    dataHandler.handle(row);
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.warn("Row: " + SKU.valueFrom(row), e);
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
                reportStatus(progress.toString());
                return shouldContinue();
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }

}
