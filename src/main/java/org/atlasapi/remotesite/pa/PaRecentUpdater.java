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

import static com.google.common.base.Preconditions.checkNotNull;

public class PaRecentUpdater extends PaBaseProgrammeUpdater implements Runnable {

    private final PaProgrammeDataStore fileManager;
    private final FileUploadResultStore fileUploadResultStore;
    
    public PaRecentUpdater(ExecutorService executor, PaChannelProcessor channelProcessor,
            PaProgrammeDataStore fileManager, ChannelResolver channelResolver,
            FileUploadResultStore fileUploadResultStore,
            PaScheduleVersionStore paScheduleVersionStore, Mode mode) {
        super(
                executor,
                channelProcessor,
                fileManager,
                channelResolver,
                Optional.of(paScheduleVersionStore),
                mode
        );
        this.fileManager = fileManager;
        this.fileUploadResultStore = fileUploadResultStore;
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
        if (mode == Mode.NORMAL) {
            return new UnprocessedFileFilter(
                    fileUploadResultStore,
                    SERVICE,
                    new DateTime(DateTimeZones.UTC).minusDays(10).getMillis()
            );
        } else {
            return input -> input.lastModified() >
                    new DateTime(DateTimeZones.UTC).minusDays(5).getMillis();
        }
    }
}
