package org.atlasapi.remotesite.amazon;


import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public interface AmazonItemProcessor {

    void prepare(OwlTelescopeReporter telescope);
    
    void process(AmazonItem item);

    void finish();
}
