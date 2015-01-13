package org.atlasapi.remotesite.opta.events.sports.model;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.gson.annotations.SerializedName;


public class SportsMatchInfo {

    @SerializedName("Date")
    private MatchDate date;
    @SerializedName("TZ")
    private String timeZone;
    @SerializedName("@attributes")
    private MatchInfoAttributes attributes;
    
    public SportsMatchInfo() { }
    
    public MatchDate date() {
        return date;
    }
    
    public String timeZone() {
        return timeZone;
    }
    
    public MatchInfoAttributes attributes() {
        return attributes;
    }

    public static class MatchInfoAttributes {
        
        @SerializedName("GroupName")
        private String groupName;
        @SerializedName("MatchDay")
        private String matchDay;
        @SerializedName("MatchType")
        private String matchType;
        @SerializedName("Period")
        private String period;
        @SerializedName("RoundNumber")
        private String roundNumber;
        @SerializedName("RoundType")
        private String roundType;
        @SerializedName("Venue_id")
        private String venueId;
        
        public MatchInfoAttributes() { }
        
        public String groupName() {
            return groupName;
        }
        
        public String matchDay() {
            return matchDay;
        }
        
        public String matchType() {
            return matchType;
        }

        public String period() {
            return period;
        }

        public String roundNumber() {
            return roundNumber;
        }

        public String roundType() {
            return roundType;
        }

        public String venueId() {
            return venueId;
        }
    }
    
    /**
     * This class wraps a date string, and simply allows use of a custom deserializer
     * for dates that can deal with Opta formatting dates either as plain strings or 
     * within JSON objects depending upon which record/feed is being parsed.
     *  
     * @author Oliver Hall (oli@metabroadcast.com)
     *
     */
    public static class MatchDate {
        
        private String date;
        
        public MatchDate(String date) { 
            this.date = checkNotNull(date);
        }
        
        public String date() {
            return date;
        }
    }
}
