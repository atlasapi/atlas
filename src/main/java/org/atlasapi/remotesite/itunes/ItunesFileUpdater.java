package org.atlasapi.remotesite.itunes;

import com.metabroadcast.common.scheduling.ScheduledTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItunesFileUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ItunesFileUpdater.class);

    private final ItunesEpfFileUpdater fileUpdater;

    private ItunesFileUpdater(ItunesEpfFileUpdater fileUpdater) {
        this.fileUpdater = checkNotNull(fileUpdater);
    }

    public static ItunesFileUpdater create(ItunesEpfFileUpdater fileUpdater) {
        return new ItunesFileUpdater(fileUpdater);
    }

    @Override
    public void runTask() {
        try {
            fileUpdater.updateEpfFiles();
        } catch (Exception e) {
            log.error("Error when updating files from the iTunes feed site", e);
        }
    }
}