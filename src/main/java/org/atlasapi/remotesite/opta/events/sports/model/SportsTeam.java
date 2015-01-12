package org.atlasapi.remotesite.opta.events.sports.model;

import org.atlasapi.remotesite.opta.events.model.OptaTeam;

import com.google.gson.annotations.SerializedName;


// TODO combine this with SoccerTeam
public class SportsTeam implements OptaTeam {

    @SerializedName("Name")
    private String name;
    @SerializedName("@attributes")
    private Attributes attributes;
    
    public SportsTeam() { }
    
    public String name() {
        return name;
    }
    
    public Attributes attributes() {
        return attributes;
    }

    public static class Attributes {
        
        @SerializedName("uID")
        private String uId;
        
        public Attributes() { }
        
        public String uId() {
            return uId;
        }
    }
}
