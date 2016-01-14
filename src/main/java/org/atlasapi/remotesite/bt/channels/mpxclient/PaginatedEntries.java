package org.atlasapi.remotesite.bt.channels.mpxclient;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;


public class PaginatedEntries {

    private int startIndex;
    private int itemsPerPage;
    private int entryCount;
    private String title;
    private List<Entry> entries;
    
    public PaginatedEntries() {
        
    }
    
    @VisibleForTesting
    public PaginatedEntries(int startIndex, int itemsPerPage, int entryCount,
            String title, Iterable<Entry> entries) {
        
        this.startIndex = startIndex;
        this.itemsPerPage = itemsPerPage;
        this.entryCount = entryCount;
        this.title = title;
        this.entries = ImmutableList.copyOf(entries);
    }
    
    public int getStartIndex() {
        return startIndex;
    }
    
    public int getItemsPerPage() {
        return itemsPerPage;
    }
    
    public int getEntryCount() {
        return entryCount;
    }
    
    public String getTitle() {
        return title;
    }
    
    public List<Entry> getEntries() {
        return entries;
    }
}
