package org.atlasapi.remotesite.opta.events.sports.model;

import java.util.List;

import org.atlasapi.remotesite.opta.events.model.OptaMatch;


public class SportsMatchData implements OptaMatch {

    private SportsMatchInfo matchInformation;
    private List<SportsStats> stats;
    private List<SportsTeamData> teamData;
    private MatchDataAttributes attributes;
    
    public SportsMatchData(SportsMatchInfo matchInformation, List<SportsStats> stats,
            List<SportsTeamData> teamData, MatchDataAttributes attributes) {
        this.matchInformation = matchInformation;
        this.stats = stats;
        this.teamData = teamData;
        this.attributes = attributes;
    }

    public SportsMatchInfo matchInformation() {
        return matchInformation;
    }
    
    public List<SportsStats> stats() {
        return stats;
    }
    
    public List<SportsTeamData> teamData() {
        return teamData;
    }
    
    public MatchDataAttributes attributes() {
        return attributes;
    }
}
