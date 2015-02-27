package org.atlasapi.remotesite.bt.events.model;

import java.util.List;


public class BtResponse {

    private int numFound;
    private int start;
    private float maxScore;
    private List<BtEvent> docs;
    
    public BtResponse() { }

    public int numFound() {
        return numFound;
    }
    
    public int start() {
        return start;
    }
    
    public float maxScore() {
        return maxScore;
    }
    
    public List<BtEvent> docs() {
        return docs;
    }
}
