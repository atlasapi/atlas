package org.atlasapi.remotesite.bt.channels.mpxclient;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import org.joda.time.DateTime;


public class Entry {

    // required for GSON
    public Entry() {
        
    }
    
    @VisibleForTesting
    public Entry(String guid, long updated, String title, Iterable<Category> categories, 
            Iterable<Content> content, boolean approved, String label, String scheme, 
            boolean isStreamable, boolean hasOutputProtection, DateTime availableDate, String linearEpgChannelId) {
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
        this.availableDate = availableDate;
        this.linearEpgChannelId = linearEpgChannelId;
    }
    
    private String guid;
    private long updated;
    private String title;
    private List<Category> categories;
    private List<Content> content;
    private boolean approved;
    private String scheme;
    private String label;

    @SerializedName("media$availableDate")
    private DateTime availableDate;
    
    @SerializedName("plproductmetadata$linearChannelNumber")
    private String linearChannelNumber;
    
    @SerializedName("plproductmetadata$linearIsStreamable")
    private boolean isStreamable;
    
    @SerializedName("plproductmetadata$linearOutputProtection")
    private boolean hasOutputProtection;

    @SerializedName("plproductmetadata$linearEpgChannelId")
    private String linearEpgChannelId;

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

    public DateTime getAvailableDate() {
        return availableDate;
    }

    public String getLinearEpgChannelId() {
        return linearEpgChannelId;
    }
}
