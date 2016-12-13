package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Song;

public class SongMerger {

    private SongMerger(){

    }

    public static SongMerger create(){
        return new SongMerger();
    }

    public Song mergeSongs(Song existing, Song update) {
        existing.setIsrc(update.getIsrc());
        existing.setDuration(update.getDuration());
        return existing;
    }
}
