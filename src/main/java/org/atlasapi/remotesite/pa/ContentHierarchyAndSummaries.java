package org.atlasapi.remotesite.pa;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;


public class ContentHierarchyAndSummaries {

    private final Optional<Brand> brand;
    private final Optional<Series> series;
    private final Item item;
    private final Broadcast broadcast;
    private final Optional<Brand> brandSummary;
    private final Optional<Series> seriesSummary;

    public ContentHierarchyAndSummaries(
            Optional<Brand> brand,
            Optional<Series> series,
            Item item,
            Broadcast broadcast,
            Optional<Brand> brandSummary,
            Optional<Series> seriesSummary) {

        this.brand = checkNotNull(brand);
        this.series = checkNotNull(series);
        this.item = checkNotNull(item);
        this.broadcast = checkNotNull(broadcast);
        this.seriesSummary = checkNotNull(seriesSummary);
        this.brandSummary = checkNotNull(brandSummary);
        
    }
    
    public Optional<Brand> getBrandSummary() {
        return brandSummary;
    }
    
    public Optional<Series> getSeriesSummary() {
        return seriesSummary;
    }

    public Optional<Brand> getBrand() {
        return this.brand;
    }

    public Optional<Series> getSeries() {
        return this.series;
    }

    public Item getItem() {
        return this.item;
    }

    public Broadcast getBroadcast() {
        return this.broadcast;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ContentHierarchyAndSummaries) {
            ContentHierarchyAndSummaries other = (ContentHierarchyAndSummaries) that;
            return broadcast.equals(other.broadcast);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return broadcast.hashCode();
    }

    @Override
    public String toString() {
        return item.getCanonicalUri() + ": " + broadcast.getSourceId();
    }
}

