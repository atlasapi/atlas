package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Synopses {

    private final String longSynopsis;
    private final String mediumSynopsis;
    private final String shortSynopsis;

    @JsonCreator
    public Synopses(
            @JsonProperty("LongSynopsis") String longSynopsis,
            @JsonProperty("MediumSynopsis") String mediumSynopsis,
            @JsonProperty("ShortSynopsis") String shortSynopsis
    ) {
        this.longSynopsis = longSynopsis;
        this.mediumSynopsis = mediumSynopsis;
        this.shortSynopsis = shortSynopsis;
    }

    public String getLongSynopsis() {
        return longSynopsis;
    }

    public String getMediumSynopsis() {
        return mediumSynopsis;
    }

    public String getShortSynopsis() {
        return shortSynopsis;
    }
}
