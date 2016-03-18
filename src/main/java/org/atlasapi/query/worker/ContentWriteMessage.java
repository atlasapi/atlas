package org.atlasapi.query.worker;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ContentWriteMessage extends AbstractMessage {

    private final byte[] contentBytes;
    private final long contentid;
    private final boolean shouldMerge;

    @JsonCreator
    public ContentWriteMessage(String messageId, Timestamp timestamp, byte[] contentBytes,
            long contentId, boolean shouldMerge) {
        super(messageId, timestamp);
        this.contentBytes = contentBytes;
        this.contentid = contentId;
        this.shouldMerge = shouldMerge;
    }

    public byte[] getContentBytes() {
        return contentBytes;
    }

    public Long getContentid() {
        return contentid;
    }

    public boolean getShouldMerge() {
        return shouldMerge;
    }
}
