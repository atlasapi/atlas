package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import java.util.Optional;

public interface ContentFactory<B, S, I> {

    Optional<Brand> createBrand(B remote);

    Optional<Clip> createClip(I remote);

    Optional<Episode> createEpisode(I remote);

    Optional<Item> createItem(I remote);

    Optional<Series> createSeries(S remote);

}