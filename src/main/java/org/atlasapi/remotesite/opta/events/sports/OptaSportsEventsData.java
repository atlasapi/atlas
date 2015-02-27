package org.atlasapi.remotesite.opta.events.sports;

import java.util.List;

import org.atlasapi.remotesite.opta.events.OptaEventsData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;

import com.google.common.collect.ImmutableList;


public class OptaSportsEventsData implements OptaEventsData<SportsTeam, SportsMatchData> {

    private final List<SportsMatchData> fixtures;
    private final List<SportsTeam> teams;
    
    public OptaSportsEventsData(Iterable<SportsMatchData> fixtures, Iterable<SportsTeam> teams) {
        this.fixtures = ImmutableList.copyOf(fixtures);
        this.teams = ImmutableList.copyOf(teams);
    }
    
    public Iterable<SportsMatchData> matches() {
        return fixtures;
    }
    
    public Iterable<SportsTeam> teams() {
        return teams;
    }
}
