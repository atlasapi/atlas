package org.atlasapi.query.worker;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ContentWriteMessageConfiguration {

    @JsonCreator
    public ContentWriteMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("contentBytes") byte[] contentBytes,
            @JsonProperty("contentId") Long contentId,
            @JsonProperty("shouldMerge") boolean shouldMerge
    ) {

    }
}
