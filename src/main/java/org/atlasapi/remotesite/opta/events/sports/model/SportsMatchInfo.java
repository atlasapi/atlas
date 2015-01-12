package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class SportsMatchInfo {

    @SerializedName("Date")
    private String date;
    @SerializedName("TZ")
    private String timeZone;
    @SerializedName("@attributes")
    private MatchInfoAttributes attributes;
    
    public SportsMatchInfo() { }
    
    public String date() {
        return date;
    }
    
    public String timeZone() {
        return timeZone;
    }
    
    public MatchInfoAttributes attributes() {
        return attributes;
    }

    public static class MatchInfoAttributes {
        
        @SerializedName("MatchDay")
        private String matchDay;
        @SerializedName("MatchType")
        private String matchType;
        @SerializedName("Period")
        private String period;
        @SerializedName("Venue_id")
        private String venueId;
        
        public MatchInfoAttributes() { }
        
        public String matchDay() {
            return matchDay;
        }
        
        public String matchType() {
            return matchType;
        }

        public String period() {
            return period;
        }

        public String venueId() {
            return venueId;
        }
    }
}
