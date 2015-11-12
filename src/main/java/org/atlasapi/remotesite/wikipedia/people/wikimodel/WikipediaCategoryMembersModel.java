package org.atlasapi.remotesite.wikipedia.people.wikimodel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class WikipediaCategoryMembersModel {
    private String batchcomplete;
    private Continue continueWiki;
    private Query query;

    @JsonCreator
    public WikipediaCategoryMembersModel(
            @JsonProperty("batchcomplete") String batchcomplete,
            @JsonProperty("continue") Continue continueWiki,
            @JsonProperty("query") Query query
    ) {
        this.batchcomplete = batchcomplete;
        this.continueWiki = continueWiki;
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    @JsonProperty("continue")
    public Continue getContinueWiki() {
        return continueWiki;
    }

    public String getBatchcomplete() {
        return batchcomplete;
    }

    public List<String> getAllTitles() {
        ImmutableList.Builder<String> titles = ImmutableList.builder();
        for (CategoryMembers member: query.getCategorymembers()) {
            titles.add(member.getTitle());
        }
        return titles.build();
    }

}
