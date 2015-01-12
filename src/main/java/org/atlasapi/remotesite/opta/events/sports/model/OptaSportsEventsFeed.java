package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class OptaSportsEventsFeed {

    @SerializedName("OptaDocument")
    private OptaDocument feed;
    
    public OptaSportsEventsFeed() { }
    
    public OptaDocument feed() {
        return feed;
    }
    
    public static class OptaDocument {
        
        @SerializedName("OptaDocument")
        private OptaSportsDocument document;
        
        public OptaDocument() { }
        
        public OptaSportsDocument document() {
            return document;
        }
    }
}
