package org.atlasapi.remotesite.amazon;


public interface AmazonProcessor<T> {

    boolean process(AmazonItem aUItem);
    
    T getResult();
}
