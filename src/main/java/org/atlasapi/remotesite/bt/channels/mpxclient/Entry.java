package org.atlasapi.remotesite.bt.channels.mpxclient;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;


public class Entry {

    // required for GSON
    public Entry() {
        
    }
    
    @VisibleForTesting
    public Entry(String guid, long updated, String title, Iterable<Category> categories, 
            Iterable<Content> content, boolean approved, String label, String scheme, 
            boolean isStreamable, boolean hasOutputProtection, String linearChannelNumber) {
        this.guid = guid;
        this.updated = updated;
        this.title = title;
        this.categories = ImmutableList.copyOf(categories);
        this.content = ImmutableList.copyOf(content);
        this.approved = approved;
        this.label = label;
        this.scheme = scheme;
        this.hasOutputProtection = hasOutputProtection;
        this.isStreamable = isStreamable;
        this.linearChannelNumber = linearChannelNumber;
    }
    
    private String guid;
    private long updated;
    private String title;
    private List<Category> categories;
    private List<Content> content;
    private boolean approved;
    private String scheme;
    private String label;
    
    @SerializedName("plproductmetadata$linearChannelNumber")
    private String linearChannelNumber;
    
    @SerializedName("plproductmetadata$linearIsStreamable")
    private boolean isStreamable;
    
    @SerializedName("plproductmetadata$linearOutputProtection")
    private boolean hasOutputProtection;

    public String getGuid() {
        return guid;
    }

    public String getScheme() {
        return scheme;
    }
    
    public String getLabel() {
        return label;
    }
    
    public long getUpdated() {
        return updated;
    }

    
    public String getTitle() {
        return title;
    }

    
    public List<Category> getCategories() {
        return categories;
    }

    
    public List<Content> getContent() {
        return content;
    }

    
    public boolean isApproved() {
        return approved;
    }

    
    public String getLinearChannelNumber() {
        return linearChannelNumber;
    }

    
    public boolean isStreamable() {
        return isStreamable;
    }

    
    public boolean hasOutputProtection() {
        return hasOutputProtection;
    }
    
}
