package org.atlasapi.remotesite.btvod.model;

import java.util.List;

public class BtVodResponse {

    private Integer startIndex;
    private Integer itemsPerPage;
    private Integer entryCount;

    private List<BtVodEntry> entries;

    public BtVodResponse() {}

    public Integer getStartIndex() {
        return startIndex;
    }

    public Integer getItemsPerPage() {
        return itemsPerPage;
    }

    public Integer getEntryCount() {
        return entryCount;
    }

    public List<BtVodEntry> getEntries() {
        return entries;
    }
}
