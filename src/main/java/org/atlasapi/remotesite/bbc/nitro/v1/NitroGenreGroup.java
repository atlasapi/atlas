package org.atlasapi.remotesite.bbc.nitro.v1;

import com.google.api.client.util.Key;
import org.joda.time.DateTime;

import java.util.List;

public class NitroGenreGroup {

    @Key private String id;
    @Key private DateTime updatedTime;
    @Key private String type;
    @Key private List<NitroGenre> genres;

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<NitroGenre> getGenres() {
        return genres;
    }

    public void setGenres(List<NitroGenre> genres) {
        this.genres = genres;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof NitroGenreGroup) {
            NitroGenreGroup other = (NitroGenreGroup) that;
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
        return String.format("%s: %s", id, genres);
    }
    
}
