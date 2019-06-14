package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class C4Com {

    private final Synopses synopses;
    private final String title;

    @JsonCreator
    public C4Com(
            @JsonProperty("Synopses") Synopses synopses,
            @JsonProperty("Title") String title
    ) {
        this.synopses = synopses;
        this.title = title;
    }

    public Synopses getSynopses() {
        return synopses;
    }

    public String getTitle() {
        return title;
    }
}
