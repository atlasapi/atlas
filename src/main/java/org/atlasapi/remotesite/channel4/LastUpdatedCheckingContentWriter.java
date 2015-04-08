package org.atlasapi.remotesite.channel4;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;

public class LastUpdatedCheckingContentWriter implements ContentWriter {

    private final AdapterLog log;
    private final ContentWriter contentWriter;
    
    public LastUpdatedCheckingContentWriter(AdapterLog log, ContentWriter contentWriter) {
        this.log = log;
        this.contentWriter = contentWriter;
    }

    @Override
    public void createOrUpdate(Container container) {
        contentWriter.createOrUpdate(checkLastUpdated(container));
    }
    
    @Override
    public Item createOrUpdate(Item item) {
        return contentWriter.createOrUpdate(checkLastUpdated(item));
    }

    private <T extends Content> T checkLastUpdated(T content) {
        if(content.getLastUpdated() == null) {
            log.record(AdapterLogEntry.warnEntry().withDescription("%s has null lastUpdated", content).withSource(getClass()));
        }
        return content;
    }
}
