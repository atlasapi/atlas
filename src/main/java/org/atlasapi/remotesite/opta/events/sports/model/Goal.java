package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class Goal {

    @SerializedName("@value")
    private String value;
    @SerializedName("@attributes")
    private GoalAttributes attributes;

    public Goal() { }
    
    public String value() {
        return value;
    }
    
    public GoalAttributes attributes() {
        return attributes;
    }
    
    public static class GoalAttributes {

        @SerializedName("Period")
        private String period;
        @SerializedName("PlayerRef")
        private String playerRef;
        @SerializedName("Type")
        private String type;
        
        public GoalAttributes() { }

        public String period() {
            return period;
        }
        
        public String playerRef() {
            return playerRef;
        }

        public String type() {
            return type;
        }
    }
}
