package org.atlasapi.equiv.update.updaters.configuration;

import org.junit.Test;

public class UpdaterConfigurationRegistryTest {

    @Test
    public void configurationIsInitialisedWithoutException() throws Exception {
        // i.e. validation passes
        UpdaterConfigurationRegistry.create();
    }
}
