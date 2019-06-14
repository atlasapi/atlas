package org.atlasapi.reporting;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.status.client.http.HttpExecutor;
import com.metabroadcast.common.http.apache.CustomHttpClientBuilder;
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
                        CustomHttpClientBuilder.create().build(),
                        Configurer.get("status.client.host").get(),
                        Configurer.get("status.client.port").toInt()),
                appId);
    }

    public OwlStatusReporter getStatusReporter() { return statusReporter; }

    public OwlTelescopeReporter getTelescopeReporter() { return telescopeReporter; }
}
