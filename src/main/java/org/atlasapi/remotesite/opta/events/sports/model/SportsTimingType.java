package org.atlasapi.remotesite.opta.events.sports.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;


public class SportsTimingType {
    
    @SerializedName("DetailTypes")
    private DetailTypes detailTypes;
    @SerializedName("TimestampAccuracyTypes")
    private TimestampAccuracyTypes timestampAccuracyTypes;
    @SerializedName("TimingType")
    private TimingType timingType;
    
    public SportsTimingType() { }
    
    public DetailTypes detailTypes() {
        return detailTypes;
    }
    
    public TimestampAccuracyTypes timestampAccuracyTypes() {
        return timestampAccuracyTypes;
    }
    
    public TimingType timingType() {
        return timingType;
    }
    
    public static class DetailTypes {
        
        @SerializedName("DetailType")
        private List<DetailType> detailTypes;
        
        public DetailTypes() { }
        
        public List<DetailType> detailTypes() { 
            return detailTypes;
        }
    }
    
    public static class TimestampAccuracyTypes {
        
        @SerializedName("TimestampAccuracyType")
        private List<TimestampAccuracyType> timestampAccuracyTypes;
        
        public TimestampAccuracyTypes() { }
        
        public List<TimestampAccuracyType> timestampAccuracyTypes() { 
            return timestampAccuracyTypes;
        }
    }
    
    public static class TimingType {
        
        @SerializedName("TimingType")
        private List<InnerTimingType> timingTypes;
        
        public TimingType() { }
        
        public List<InnerTimingType> timingTypes() { 
            return timingTypes;
        }
    }
}