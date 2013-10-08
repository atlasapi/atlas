package org.atlasapi.remotesite.amazonunbox;


public interface AmazonUnboxItemProcessor {

    void prepare();
    
    void process(AmazonUnboxItem item);
    
    void finish();
}
