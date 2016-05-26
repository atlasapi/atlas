package org.atlasapi.remotesite.pa;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ContentHierarchyAndBroadcast;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;


public class ContentHierarchyAndSummaries extends ContentHierarchyAndBroadcast {
    
    private final Optional<Brand> brandSummary;
    private final Optional<Series> seriesSummary;

    public ContentHierarchyAndSummaries(
            Optional<Brand> brand,
            Optional<Series> series,
            Item item,
            Broadcast broadcast,
            Optional<Brand> brandSummary,
            Optional<Series> seriesSummary) {
        
        super(brand, series, item, broadcast);
        this.seriesSummary = checkNotNull(seriesSummary);
        this.brandSummary = checkNotNull(brandSummary);
        
    }
    
    public Optional<Brand> getBrandSummary() {
        return brandSummary;
    }
    
    public Optional<Series> getSeriesSummary() {
        return seriesSummary;
    }
    
}
