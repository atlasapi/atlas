package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class Brand {

    private final C4Com c4Com;

    @JsonCreator
    public Brand(
            @JsonProperty("C4Com") C4Com c4Com
    ) {
        this.c4Com = checkNotNull(c4Com);
    }

    public C4Com getC4Com() {
        return c4Com;
    }
}
