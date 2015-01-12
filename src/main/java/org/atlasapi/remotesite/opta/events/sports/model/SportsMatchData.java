package org.atlasapi.remotesite.opta.events.sports.model;

import java.util.List;

import org.atlasapi.remotesite.opta.events.model.OptaMatch;
import org.atlasapi.remotesite.opta.events.soccer.model.MatchDataAttributes;

import com.google.gson.annotations.SerializedName;


public class SportsMatchData implements OptaMatch {

    @SerializedName("MatchInfo")
    private SportsMatchInfo matchInformation;
    @SerializedName("Stat")
    private SportsStats stats;
    @SerializedName("TeamData")
    private List<SportsTeamData> teamData;
    @SerializedName("@attributes")
    private MatchDataAttributes attributes;
    
    public SportsMatchData() { }
    
    public SportsMatchInfo matchInformation() {
        return matchInformation;
    }
    
    public SportsStats stats() {
        return stats;
    }
    
    public List<SportsTeamData> teamData() {
        return teamData;
    }
    
    public MatchDataAttributes attributes() {
        return attributes;
    }
}
