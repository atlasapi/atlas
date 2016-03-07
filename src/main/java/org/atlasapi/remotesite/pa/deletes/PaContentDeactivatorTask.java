package org.atlasapi.remotesite.pa.deletes;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaContentDeactivatorTask extends ScheduledTask {

    private final PaContentDeactivator deactivator;
    private final PaProgrammeDataStore paDataStore;

    public PaContentDeactivatorTask(PaContentDeactivator deactivator,
                                    PaProgrammeDataStore paDataStore) {
        this.deactivator = checkNotNull(deactivator);
        this.paDataStore = checkNotNull(paDataStore);
        withName("PA Content Deactivator");
    }

    @Override
    protected void runTask() {
        reportStatus("Starting");
        File activeIds = findLatestActiveIdArchive(paDataStore);
        try {
            deactivator.deactivate(activeIds, false, reporter());
        } catch (IOException e) {
            reportStatus(String.format("Failed with exception %s", e));
        }
    }

    private File findLatestActiveIdArchive(PaProgrammeDataStore paDataStore) {
        List<File> archiveIdFiles = paDataStore.localActiveIdArchiveFiles(null);
        return checkNotNull(Iterables.getFirst(archiveIdFiles, null));
    }
}
