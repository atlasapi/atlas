package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Epg {

    private final String description;
    private final String title;

    @JsonCreator
    public Epg(
            @JsonProperty("Description") String description,
            @JsonProperty("Title") String title
    ) {
        this.description = description;
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public String getTitle() {
        return title;
    }
}
