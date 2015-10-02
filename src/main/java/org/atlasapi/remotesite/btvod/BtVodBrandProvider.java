package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.util.Map;

public class BtVodBrandProvider {

    private final BrandUriExtractor brandUriExtractor;
    private final Map<String, Brand> brands;

    public BtVodBrandProvider(
            BrandUriExtractor brandUriExtractor,
            Map<String, Brand> brands
    ) {
        this.brandUriExtractor = brandUriExtractor;
        this.brands = ImmutableMap.copyOf(brands);
    }


    Optional<Brand> brandFor(BtVodEntry row) {
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


}
