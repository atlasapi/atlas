package org.atlasapi.remotesite.opta.events.sports.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;


public class SportsTeamData {

    private String value;
    private List<Goal> goals;
    private TeamDataAttributes attributes;

    public SportsTeamData(String value, List<Goal> goals, 
            TeamDataAttributes attributes) {
        this.value = value;
        this.goals = goals;
        this.attributes = attributes;
    }

    public String value() {
        return value;
    }
    
    public List<Goal> goals() {
        return goals;
    }
    
    public TeamDataAttributes attributes() {
        return attributes;
    }
    
    public static class TeamDataAttributes {
        
        @SerializedName("HalfScore")
        private String halfScore;
        @SerializedName("Score")
        private String score;
        @SerializedName("Side")
        private String side;
        @SerializedName("TeamRef")
        private String teamRef;
        
        public TeamDataAttributes() { }
        
        public String halfScore() {
            return halfScore;
        }
        
        public String score() {
            return score;
        }
        
        public String side() {
            return side;
        }
        
        public String teamRef() {
            return teamRef;
        }
    }
}
