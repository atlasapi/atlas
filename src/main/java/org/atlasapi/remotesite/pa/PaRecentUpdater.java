package org.atlasapi.remotesite.pa;

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import org.joda.time.DateTime;

public class PaRecentUpdater extends PaBaseProgrammeUpdater implements Runnable {
       
    private final PaProgrammeDataStore fileManager;
    private final FileUploadResultStore fileUploadResultStore;
    private final boolean ignoreProcessedFiles;
    
    public PaRecentUpdater(ExecutorService executor, PaChannelProcessor channelProcessor,
            PaProgrammeDataStore fileManager, ChannelResolver channelResolver,
            FileUploadResultStore fileUploadResultStore,
            PaScheduleVersionStore paScheduleVersionStore, boolean ignoreProcessedFiles) {
        super(executor, channelProcessor, fileManager, channelResolver, Optional.of(paScheduleVersionStore));
        this.fileManager = fileManager;
        this.fileUploadResultStore = fileUploadResultStore;
        this.ignoreProcessedFiles = ignoreProcessedFiles;
    }
    
    @Override
    public void runTask() {
        Predicate<File> filter = getFileSelectionPredicate();
        this.processFiles(fileManager.localTvDataFiles(filter));
    }

    @Override
    protected void storeResult(FileUploadResult result) {
        fileUploadResultStore.store(result.filename(), result);
    }

    private Predicate<File> getFileSelectionPredicate() {
        if (ignoreProcessedFiles) {
            return new UnprocessedFileFilter(
                    fileUploadResultStore,
                    SERVICE,
                    new DateTime(DateTimeZones.UTC).minusDays(10).getMillis()
            );
        } else {
            return new Predicate<File>() {
                @Override
                public boolean apply(File input) {
                    return input.lastModified() >
                            new DateTime(DateTimeZones.UTC).minusDays(3).getMillis();
                }
            };
        }
    }
}
