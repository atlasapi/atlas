package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class BtVodBrandProvider {

    private final BrandUriExtractor brandUriExtractor;
    private final Map<String, Brand> brands;
    private final BrandDescriptionUpdater brandDescriptionUpdater;

    public BtVodBrandProvider(
            BrandUriExtractor brandUriExtractor,
            Map<String, Brand> brands, BrandDescriptionUpdater brandDescriptionUpdater
    ) {
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.brands = ImmutableMap.copyOf(brands);
        this.brandDescriptionUpdater = checkNotNull(brandDescriptionUpdater);
    }


    public Optional<Brand> brandFor(BtVodEntry row) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(row);
        if (brandUri.isPresent() && brands.containsKey(brandUri.get())) {
            return Optional.of(brands.get(brandUri.get()));
        }
        return Optional.absent();

    }

    public Optional<ParentRef> brandRefFor(BtVodEntry row) {
        Optional<String> optionalUri = brandUriExtractor.extractBrandUri(row);

        if (!optionalUri.isPresent() || !brands.containsKey(optionalUri.get())) {
            return Optional.absent();
        }

        return Optional.fromNullable(
                ParentRef.parentRefFrom(
                        brands.get(
                                optionalUri.get()
                        )
                )
        );
    }

    public void updateDescriptions(BtVodEntry seriesRow, Series series) {
        Optional<Brand> brandOptional = brandFor(seriesRow);
        if(!brandOptional.isPresent()) {
            return;
        }
        Brand brand = brandOptional.get();

        brandDescriptionUpdater.updateDescriptions(brand, series);
    }
}
