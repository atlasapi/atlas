package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Content;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;


public class VodEntryAndContent {

    private BtVodEntry btVodEntry;
    private Content content;

    public VodEntryAndContent(BtVodEntry btVodDataRow, Content content) {
        this.btVodEntry = checkNotNull(btVodDataRow);
        this.content = checkNotNull(content);
    }
    
    public BtVodEntry getBtVodEntry() {
        return btVodEntry;
    }
    
    public Content getContent() {
        return content;
    }
}
