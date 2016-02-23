package org.atlasapi.remotesite.pa;

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

@Controller
public class PaSingleDateUpdater extends PaBaseProgrammeUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(PaSingleDateUpdater.class);
    
    private final String dateString;
    private final PaProgrammeDataStore fileManager;

    public PaSingleDateUpdater(ExecutorService executor, PaChannelProcessor channelProcessor, PaProgrammeDataStore fileManager, ChannelResolver channelResolver, String dateString) {
        super(executor, channelProcessor, fileManager, channelResolver, Optional.<PaScheduleVersionStore>absent());
        this.fileManager = fileManager;
        this.dateString = dateString;
    }

    @Override
    public void runTask() {
        LOG.info("Beginning ingest of PA files for {}", dateString);

    	final String filenameContains = dateString + "_tvdata";
        processFiles(fileManager.localTvDataFiles(new Predicate<File>() {
            @Override
            public boolean apply(File input) {
                return input.getName().contains(filenameContains);
            }
        }));

        LOG.info("Finished ingest of PA files for {}", dateString);
    }
}
