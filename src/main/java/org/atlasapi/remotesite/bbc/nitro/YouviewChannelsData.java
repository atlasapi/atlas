package org.atlasapi.remotesite.bbc.nitro;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class YouviewChannelsData {

    private Set<YouviewService> services;

    @JsonCreator
    public YouviewChannelsData(@JsonProperty Set<YouviewService> services) {
        this.services = services;
    }

    public YouviewChannelsData() {
    }

    public Set<YouviewService> getServices() {
        return services;
    }

    public void setServices(
            Set<YouviewService> services) {
        this.services = services;
    }

}
