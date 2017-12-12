package org.atlasapi.reporting;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.status.client.http.HttpExecutor;
import com.metabroadcast.status.client.http.RetryStrategy;
import org.apache.http.impl.client.HttpClients;
import org.atlasapi.reporting.status.OwlStatusReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public class OwlReporter {

    private final OwlTelescopeReporter telescopeReporter;
    private OwlStatusReporter statusReporter;

    private String appId = "d4r";

    public OwlReporter(OwlTelescopeReporter telescopeReporter){
        this.telescopeReporter = telescopeReporter;
        this.statusReporter = new OwlStatusReporter(
                HttpExecutor.create(
                        HttpClients.custom()
                            .setServiceUnavailableRetryStrategy(new RetryStrategy())
                            .build(),
                        Configurer.get("status.service.host").get(),
                        Configurer.get("status.service.port").toInt()),
                appId);
    }

    public OwlStatusReporter getStatusReporter() { return statusReporter; }

    public OwlTelescopeReporter getTelescopeReporter() { return telescopeReporter; }
}
