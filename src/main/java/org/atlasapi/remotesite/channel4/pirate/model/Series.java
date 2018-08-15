package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class Series {

    private final C4Com c4Com;
    private final Epg epg;

    @JsonCreator
    public Series(
            @JsonProperty("C4Com") C4Com c4Com,
            @JsonProperty("Epg") Epg epg
    ) {
        this.c4Com = c4Com;
        this.epg = epg;
    }

    public C4Com getC4Com() {
        return c4Com;
    }

    public Epg getEpg() {
        return epg;
    }

}
