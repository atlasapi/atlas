package org.atlasapi.remotesite.barb.channels.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class BarbChannelIngestResponse {

    @JsonProperty("created_channels")
    private final Map<String, String> createdChannels;
    @JsonProperty("updated_channels")
    private final List<String> updatedChannels;
    @JsonProperty("failed_channels")
    private final List<String> failedChannels;
    @JsonProperty("message")
    private final String message;

    private BarbChannelIngestResponse(
            Map<String, String> createdChannels,
            List<String> updatedChannels,
            List<String> failedChannels,
            String message
    ) {
        this.createdChannels = createdChannels;
        this.updatedChannels = updatedChannels;
        this.failedChannels = failedChannels;
        this.message = message;
    }

    @JsonCreator
    public static BarbChannelIngestResponse create(
            Map<String, String> createdChannels,
            List<String> updatedChannels,
            List<String> failedChannels,
            String message
    ) {
        return new BarbChannelIngestResponse(createdChannels, updatedChannels, failedChannels, message);
    }

    @JsonProperty("created_channels")
    public Map<String, String> getCreatedChannels() {
        return ImmutableMap.copyOf(createdChannels);
    }

    @JsonProperty("updated_channels")
    public List<String> getUpdatedChannels() {
        return ImmutableList.copyOf(updatedChannels);
    }

    @JsonProperty("failed_channels")
    public List<String> getFailedChannels() {
        return ImmutableList.copyOf(failedChannels);
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }
}