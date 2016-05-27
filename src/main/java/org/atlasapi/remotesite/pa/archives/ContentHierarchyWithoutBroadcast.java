package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Optional;

public class ContentHierarchyWithoutBroadcast {

    private final Optional<Brand> brand;
    private final Optional<Series> series;
    private final Item item;
    private final Optional<Brand> brandSummary;
    private final Optional<Series> seriesSummary;

    public ContentHierarchyWithoutBroadcast(
            Optional<Brand> brand,
            Optional<Series> series,
            Item item,
            Optional<Brand> brandSummary,
            Optional<Series> seriesSummary
    ) {
        this.brand = brand;
        this.series = series;
        this.item = item;
        this.brandSummary = brandSummary;
        this.seriesSummary = seriesSummary;
    }

    public Optional<Brand> getBrand() {
        return brand;
    }

    public Optional<Series> getSeries() {
        return series;
    }

    public Item getItem() {
        return item;
    }

    public Optional<Brand> getBrandSummary() {
        return brandSummary;
    }

    public Optional<Series> getSeriesSummary() {
        return seriesSummary;
    }
}
