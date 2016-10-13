package org.atlasapi.remotesite.bbc.nitro.channels.hax;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LocatorWithRegions {

    private final String locator;
    private final List<String> regions;

    @JsonCreator
    static LocatorWithRegions create(
            @JsonProperty("locator") String locator,
            @JsonProperty("regions") List<String> regions
    ) {
        return new LocatorWithRegions(locator, regions);
    }

    private LocatorWithRegions(String locator, List<String> regions) {
        this.locator = locator;
        this.regions = regions;
    }

    @JsonProperty("locator")
    public String getLocator() {
        return locator;
    }

    @JsonProperty("regions")
    public List<String> getRegions() {
        return regions;
    }
}
