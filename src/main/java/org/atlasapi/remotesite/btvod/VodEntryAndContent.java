package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import org.atlasapi.media.entity.Content;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;


public class VodEntryAndContent {

    private BtVodEntry btVodEntry;
    private Content content;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VodEntryAndContent that = (VodEntryAndContent) o;
        return Objects.equal(btVodEntry, that.btVodEntry) &&
                Objects.equal(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(btVodEntry, content);
    }

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
