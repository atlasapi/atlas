package org.atlasapi.equiv.handlers;

import java.util.Optional;

import org.atlasapi.equiv.ColumbusTelescopeReporter;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class ColumbusTelescopeReportHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private final ColumbusTelescopeReporter columbusTelescopeReporter;

    public ColumbusTelescopeReportHandler() {
        this.columbusTelescopeReporter = new ColumbusTelescopeReporter();
    }

    @Override
    public void handle(
            EquivalenceResult<T> result,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        columbusTelescopeReporter.reportItem(
                taskId,
                result,
                telescopeClient
        );
    }
}