package org.atlasapi.remotesite.amazonunbox;


import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public interface AmazonUnboxItemProcessor {

    void prepare();
    
    void process(AmazonUnboxItem item);

    void finish(OwlTelescopeReporter telescope);
}
