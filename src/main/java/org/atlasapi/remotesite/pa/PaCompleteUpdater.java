package org.atlasapi.remotesite.pa;

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;

public class PaCompleteUpdater extends PaBaseProgrammeUpdater implements Runnable {
    
    private final PaProgrammeDataStore fileManager;

    public PaCompleteUpdater(ExecutorService executor, PaChannelProcessor processor, PaProgrammeDataStore fileManager, ChannelResolver channelResolver) {
        super(
                executor,
                processor,
                fileManager,
                channelResolver,
                Optional.absent(),
                Mode.BOOTSTRAP
        );
        this.fileManager = fileManager;
    }
    
    @Override
    public void runTask() {
        this.processFiles(fileManager.localTvDataFiles(Predicates.alwaysTrue()));
    }
    
}
