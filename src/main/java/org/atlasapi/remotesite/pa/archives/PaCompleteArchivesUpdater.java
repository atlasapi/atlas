package org.atlasapi.remotesite.pa.archives;

import java.io.File;

import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;

import com.google.common.base.Predicates;

public class PaCompleteArchivesUpdater extends PaArchivesUpdater {

    private final PaProgrammeDataStore fileManager;

    public PaCompleteArchivesUpdater(PaProgrammeDataStore dataStore, FileUploadResultStore fileUploadResultStore, PaUpdatesProcessor processor) {
        super(dataStore, fileUploadResultStore, processor);
        fileManager = dataStore;
    }

    @Override
    public void runTask() {
        this.processFiles(fileManager.localTvDataFiles(Predicates.<File>alwaysTrue()));
    }

}
