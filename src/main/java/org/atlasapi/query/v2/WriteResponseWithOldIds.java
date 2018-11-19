package org.atlasapi.query.v2;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WriteResponseWithOldIds {

    private final String id;
    private final String deerId;

    @JsonCreator
    public WriteResponseWithOldIds(
            @JsonProperty("id") String id
    ) {
        this.id = id;
        this.deerId = "";
    }

    @JsonCreator
    public WriteResponseWithOldIds(
            @JsonProperty("id") String id,
            @JsonProperty("v4-id") String deerId
    ) {
        this.id = id;
        this.deerId = deerId;
    }

    public String getId() {
        return id;
    }

    public String getDeerId() {
        return deerId;
    }
}
