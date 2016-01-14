package org.atlasapi.equiv;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.system.PaAliasBackPopulator;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaAliasBackPopulatorTask extends ScheduledTask {

    private final PaAliasBackPopulator backPopulator;

    public PaAliasBackPopulatorTask(PaAliasBackPopulator backPopulator) {
        this.backPopulator = checkNotNull(backPopulator);
    }

    @Override
    protected void runTask() {
        try {
            backPopulator.backpopulate();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
