package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class SportsTeamData {

    @SerializedName("@attributes")
    private TeamDataAttributes attributes;

    public SportsTeamData() { }
    
    public TeamDataAttributes attributes() {
        return attributes;
    }
    
    public static class TeamDataAttributes {
        
        @SerializedName("Score")
        private String score;
        @SerializedName("Side")
        private String side;
        @SerializedName("TeamRef")
        private String teamRef;
        
        public TeamDataAttributes() { }
        
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
