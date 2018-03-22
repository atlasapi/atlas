package org.atlasapi.remotesite.channel4.pmlsd.epg;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Series;

import java.util.Optional;

public class C4EpgEntryItemSource {

    private final C4EpgChannelEntry entry;
    private final Optional<Brand> brand;
    private final Optional<Series> series;

    public C4EpgEntryItemSource(
            C4EpgChannelEntry entry,
            Optional<Brand> brand,
            Optional<Series> series) {
        this.entry = entry;
        this.brand = brand;
        this.series = series;
    }

    public C4EpgChannelEntry getEntry() {
        return this.entry;
    }

    public Optional<Brand> getBrand() {
        return this.brand;
    }

    public Optional<Series> getSeries() {
        return this.series;
    }

}
