package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

import com.google.common.collect.SetMultimap;

public class BrandSeriesAndEpisodes {

    private final Brand brand;
    private final SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> seriesAndEpisodes;

    public BrandSeriesAndEpisodes(Brand brand, SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> seriesAndEpisodes) {
        this.brand = brand;
        this.seriesAndEpisodes = seriesAndEpisodes;
    }

    public Brand getBrand() {
        return this.brand;
    }

    public SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> getSeriesAndEpisodes() {
        return this.seriesAndEpisodes;
    }

}
