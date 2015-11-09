package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class BtVodBrandProvider {

    private final BrandUriExtractor brandUriExtractor;
    private final Map<String, Brand> brands;
    private final BrandDescriptionUpdater brandDescriptionUpdater;
    private final CertificateUpdater certificateUpdater;

    public BtVodBrandProvider(BrandUriExtractor brandUriExtractor,
            Map<String, Brand> brands, BrandDescriptionUpdater brandDescriptionUpdater,
            CertificateUpdater certificateUpdater) {
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.brands = ImmutableMap.copyOf(brands);
        this.brandDescriptionUpdater = checkNotNull(brandDescriptionUpdater);
        this.certificateUpdater = checkNotNull(certificateUpdater);
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

    public void updateBrandFromSeries(BtVodEntry seriesRow, Series series) {
        Optional<Brand> brandOptional = brandFor(seriesRow);
        if(!brandOptional.isPresent()) {
            return;
        }
        Brand brand = brandOptional.get();

        brandDescriptionUpdater.updateDescriptions(brand, series);
        certificateUpdater.updateCertificates(brand, series);
    }

    public void updateBrandFromEpisode(BtVodEntry episodeRow, Episode episode) {
        Optional<Brand> brandOptional = brandFor(episodeRow);
        if(!brandOptional.isPresent()) {
            return;
        }
        Brand brand = brandOptional.get();

        certificateUpdater.updateCertificates(brand, episode);
    }
}
