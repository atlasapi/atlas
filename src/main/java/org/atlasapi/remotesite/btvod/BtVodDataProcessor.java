package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

public interface BtVodDataProcessor<T> {

    boolean process(BtVodEntry row);
    
    T getResult();
    
}
