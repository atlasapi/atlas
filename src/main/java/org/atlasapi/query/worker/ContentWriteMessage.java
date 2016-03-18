package org.atlasapi.query.worker;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

public class ContentWriteMessage extends AbstractMessage {

    private final byte[] contentBytes;
    private final Long contentId;
    private final boolean shouldMerge;

    public ContentWriteMessage(String messageId, Timestamp timestamp, byte[] contentBytes,
            Long contentId, boolean shouldMerge) {
        super(messageId, timestamp);
        this.contentBytes = contentBytes;
        this.contentId = contentId;
        this.shouldMerge = shouldMerge;
    }

    public byte[] getContentBytes() {
        return contentBytes;
    }

    public Long getContentId() {
        return contentId;
    }

    public boolean getShouldMerge() {
        return shouldMerge;
    }
}
