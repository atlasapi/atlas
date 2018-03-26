package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Genre {

    private final String code;
    private final String description;

    @JsonCreator
    public Genre(
            @JsonProperty("Code") String code,
            @JsonProperty("Description") String description
    ) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
