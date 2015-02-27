package org.atlasapi.remotesite.bt.events.model;

import com.google.gson.annotations.SerializedName;


public class BtResponseHeader {

    private int status;
    @SerializedName("QTime")
    private int qTime;
    private BtParameters params;
    
    public BtResponseHeader() { }
    
    public int status() {
        return status;
    }
    
    public int qTime() {
        return qTime;
    }
    
    public BtParameters params() {
        return params;
    }

    public static class BtParameters {
        
        private String fq;
        private String supressModuleInfo;
        private String q;
        
        public BtParameters() { }
        
        public String fq() {
            return fq;
        }
        
        public String supressModuleInfo() {
            return supressModuleInfo;
        }
        
        public String q() {
            return q;
        }
    }
}
