package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Optional;

public class ContentHierarchyWithoutBroadcast {

    private Optional<Brand> brand;
    private Optional<Series> series;
    private Item item;

    public ContentHierarchyWithoutBroadcast(Optional<Brand> brand, Optional<Series> series, Item item) {
        this.brand = brand;
        this.series = series;
        this.item = item;
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
}
