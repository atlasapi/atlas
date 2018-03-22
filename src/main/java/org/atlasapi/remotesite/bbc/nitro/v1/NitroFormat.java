package org.atlasapi.remotesite.bbc.nitro.v1;

import com.google.api.client.util.Key;
import org.joda.time.DateTime;


public class NitroFormat {

    @Key private String id;
    @Key private DateTime updatedTime;
    @Key private String title;
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public DateTime getUpdatedTime() {
        return updatedTime;
    }
    
    public void setUpdatedTime(DateTime updatedTime) {
        this.updatedTime = updatedTime;
    }
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    } 
 
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof NitroFormat) {
            NitroFormat other = (NitroFormat) that;
            return id.equals(other.id);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("%s: %s", id, title);
    }
    
}
