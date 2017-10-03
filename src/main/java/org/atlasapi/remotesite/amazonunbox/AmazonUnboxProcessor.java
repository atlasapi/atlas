package org.atlasapi.remotesite.amazonunbox;

import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public interface AmazonUnboxProcessor<T> {

    boolean process(AmazonUnboxItem aUItem);
    
    T getResult();
}
