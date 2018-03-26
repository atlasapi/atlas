package org.atlasapi.remotesite.channel4.pirate.model;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Brand;

public class EpisodeSeriesBrand {

    private final Episode episode;
    private final Series series;
    private final Brand brand;

    public EpisodeSeriesBrand(Episode episode, Series series, Brand brand) {
        this.episode = episode;
        this.series = series;
        this.brand = brand;
    }

    public Episode getEpisode() {
        return episode;
    }

    public Series getSeries() {
        return series;
    }

    public Brand getBrand() {
        return brand;
    }
}
