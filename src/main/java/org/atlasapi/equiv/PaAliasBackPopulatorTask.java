package org.atlasapi.equiv;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.remotesite.pa.PaAliasBackPopulator;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaAliasBackPopulatorTask extends ScheduledTask {

    private final PaAliasBackPopulator backPopulator;
    private final Boolean dryRun;

    public PaAliasBackPopulatorTask(PaAliasBackPopulator backPopulator, Boolean dryRun) {
        this.backPopulator = checkNotNull(backPopulator);
        this.dryRun = checkNotNull(dryRun);
    }

    @Override
    protected void runTask() {
        try {
            backPopulator.backpopulate(reporter(), dryRun);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
