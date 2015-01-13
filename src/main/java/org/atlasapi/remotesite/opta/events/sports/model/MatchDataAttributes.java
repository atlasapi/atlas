package org.atlasapi.remotesite.opta.events.sports.model;

import com.google.gson.annotations.SerializedName;


public class MatchDataAttributes {

    @SerializedName("detail_id")
    private String detailId;
    @SerializedName("last_modified")
    private String lastModified;
    @SerializedName("timestamp_accuracy_id")
    private String timestampAccuracyId;
    @SerializedName("timing_id")
    private String timingId;
    @SerializedName("uID")
    private String uId;
    
    public MatchDataAttributes() { }

    public String detailId() {
        return detailId;
    }
    
    public String lastModified() {
        return lastModified;
    }
    
    public String timestampAccuracyId() {
        return timestampAccuracyId;
    }
    
    public String timingId() {
        return timingId;
    }
    
    public String uId() {
        return uId;
    }
}
