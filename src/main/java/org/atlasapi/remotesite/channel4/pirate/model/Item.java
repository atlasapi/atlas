package org.atlasapi.remotesite.channel4.pirate.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Item {

    private final EditorialInformation editorialInformation;

    @JsonCreator
    public Item(
            @JsonProperty("EditorialInformation") EditorialInformation editorialInformation) {
        this.editorialInformation = editorialInformation;
    }

    public EditorialInformation getEditorialInformation() {
        return editorialInformation;
    }
}
