package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Series;

public class SeriesMerger {

    private SeriesMerger(){

    }

    public static SeriesMerger create(){
        return new SeriesMerger();
    }

    public Series mergeSeries(Series existing, Series update) {
        existing.withSeriesNumber(update.getSeriesNumber());
        existing.setTotalEpisodes(update.getTotalEpisodes());
        existing.setParentRef(update.getParent());
        return existing;
    }
}
