package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class SportsStats {

    @SerializedName("@value")
    private String value;
    @SerializedName("@attributes")
    private StatsAttributes attributes;
    
    public SportsStats() { }
    
    public String value() {
        return value;
    }
    
    public StatsAttributes attributes() {
        return attributes;
    }
    
    public static class StatsAttributes {
        
        @SerializedName("Type")
        private String type;
        
        public StatsAttributes() { }
        
        public String type() {
            return type;
        }
    }
}
