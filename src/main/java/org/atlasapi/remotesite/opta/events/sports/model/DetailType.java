package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class DetailType {

    @SerializedName("@value")
    private String value;
    @SerializedName("@attributes")
    private Attributes attributes;
    
    public DetailType () { }
    
    public String value() {
        return value;
    }
    
    public Attributes attributes() {
        return attributes;
    }

    public static class Attributes {
        
        @SerializedName("detail_id")
        private String detailId;
        private String name;
        
        public Attributes() { }
        
        public String detailId() {
            return detailId;
        }
        
        public String name() {
            return name;
        }
    }
}
