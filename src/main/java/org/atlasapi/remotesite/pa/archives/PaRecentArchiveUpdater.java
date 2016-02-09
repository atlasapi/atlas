package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.remotesite.pa.UnprocessedFileFilter;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;

import com.metabroadcast.common.time.DateTimeZones;

import org.joda.time.DateTime;

public class PaRecentArchiveUpdater extends PaArchivesUpdater {

    private final PaProgrammeDataStore fileManager;
    private final FileUploadResultStore uploadResultStore;

    public PaRecentArchiveUpdater(PaProgrammeDataStore dataStore, FileUploadResultStore fileUploadResultStore, PaUpdatesProcessor processor) {
        super(dataStore, fileUploadResultStore, processor);
        fileManager = dataStore;
        this.uploadResultStore = fileUploadResultStore;
    }

    @Override
    public void runTask() {
        final Long since = new DateTime(DateTimeZones.UTC).minusDays(10).getMillis();
        this.processFiles(fileManager.localTvDataFiles(new UnprocessedFileFilter(uploadResultStore, SERVICE, since)));
    }

}
