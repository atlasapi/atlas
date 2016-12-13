package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;

public class EpisodeMerger {

    private EpisodeMerger(){

    }

    public static EpisodeMerger create(){
        return new EpisodeMerger();
    }

    public Item mergeEpisodes(Episode existing, Episode update) {
        existing.setSeriesNumber(update.getSeriesNumber());
        existing.setEpisodeNumber(update.getEpisodeNumber());
        return existing;
    }
}
