package org.atlasapi.deer.elasticsearch.content;

// I have simplified this class to avoid importing other classes we don't need
public enum ContentType {

    BRAND,
    SERIES,
    ITEM,
    EPISODE,
    FILM,
    SONG,
    CLIP,
    ;

    public String getKey() {
        return this.name().toLowerCase();
    }

    @Override
    public String toString() {
        return getKey();
    }
}
