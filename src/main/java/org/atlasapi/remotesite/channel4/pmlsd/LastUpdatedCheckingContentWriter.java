package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastUpdatedCheckingContentWriter implements ContentWriter {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentWriter contentWriter;
    
    public LastUpdatedCheckingContentWriter(ContentWriter contentWriter) {
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
            log.info("{} has null lastUpdated", content);
        }
        return content;
    }
}
