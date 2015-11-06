package org.atlasapi.remotesite.wikipedia.people.wikimodel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Query {
    private List<CategoryMembers> categorymembers;

    @JsonCreator
    public Query (
            @JsonProperty("categorymembers") List<CategoryMembers> categorymembers
    ) {
        this.categorymembers = categorymembers;
    }

    public List<CategoryMembers> getCategorymembers() {
        return categorymembers;
    }
}
