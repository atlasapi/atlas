package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class TimestampAccuracyType {

    @SerializedName("@value")
    private String value;
    @SerializedName("@attributes")
    private Attributes attributes;
    
    public TimestampAccuracyType() { }
    
    public String value() {
        return value;
    }
    
    public Attributes attributes() {
        return attributes;
    }

    public static class Attributes {
        
        @SerializedName("timing_id")
        private String timingId;
        private String name;
        
        public Attributes() { }
        
        public String timingId() {
            return timingId;
        }
        
        public String name() {
            return name;
        }
    }
}
