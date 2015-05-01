package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Content;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;


public interface BtVodContentListener {

    void onContent(Content content, BtVodEntry vodData);
    
}
