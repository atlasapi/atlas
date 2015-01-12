package org.atlasapi.remotesite.opta.events.sports.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;


public class OptaSportsDocument {

    @SerializedName("MatchData")
    private List<SportsMatchData> matchData;
    @SerializedName("Team")
    private List<SportsTeam> teams;
    @SerializedName("@attributes")
    private SportsFeedAttributes attributes;
    
    public OptaSportsDocument() { }
    
    public List<SportsMatchData> matchData() {
        return matchData;
    }
    
    public List<SportsTeam> teams() {
        return teams;
    }
    
    public SportsFeedAttributes attributes() {
        return attributes;
    }
}
