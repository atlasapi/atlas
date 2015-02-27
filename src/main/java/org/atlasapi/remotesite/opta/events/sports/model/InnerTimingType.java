package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class InnerTimingType {

    @SerializedName("@value")
    private String value;
    @SerializedName("@attributes")
    private Attributes attributes;
    
    public InnerTimingType() { }
    
    public String value() {
        return value;
    }
    
    public Attributes attributes() {
        return attributes;
    }

    public static class Attributes {
        
        @SerializedName("timestamp_accuracy_id")
        private String timestampAccuracyId;
        private String name;
        
        public Attributes() { }
        
        public String timestampAccuracyId() {
            return timestampAccuracyId;
        }
        
        public String name() {
            return name;
        }
    }
}
