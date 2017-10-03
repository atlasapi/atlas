package org.atlasapi.remotesite.amazonunbox;


public interface AmazonUnboxProcessor<T> {

    boolean process(AmazonUnboxItem aUItem);
    
    T getResult();
}
