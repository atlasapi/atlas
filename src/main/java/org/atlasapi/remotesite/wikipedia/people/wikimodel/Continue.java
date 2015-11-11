package org.atlasapi.remotesite.wikipedia.people.wikimodel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Continue {
    private String continueWiki;
    private String cmcontinue;

    @JsonCreator
    public Continue (
            @JsonProperty("continue") String continueWiki,
            @JsonProperty("cmcontinue") String cmcontinue
    ) {
        this.continueWiki = continueWiki;
        this.cmcontinue = cmcontinue;
    }

    @JsonProperty("continue")
    public String getContinueWiki() {
        return continueWiki;
    }

    public String getCmcontinue() {
        return cmcontinue;
    }
}
