package org.atlasapi.remotesite.wikipedia.people.wikimodel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CategoryMembers {
    private int pageid;
    private int ns;
    private String title;

    @JsonCreator
    public CategoryMembers (
            @JsonProperty("pageid") Integer pageid,
            @JsonProperty("ns") Integer ns,
            @JsonProperty("title") String title
    ) {
        this.pageid = pageid;
        this.ns = ns;
        this.title = title;
    }

    public Integer getPageid() {
        return pageid;
    }

    public Integer getNs() {
        return ns;
    }

    public String getTitle() {
        return title;
    }
}
