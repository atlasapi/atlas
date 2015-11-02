package org.atlasapi.remotesite.btvod;

import java.util.HashMap;
import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Optional;

public class BrandDescriptionUpdater {

    // Records the series number that is the source for the description in a brand
    private Map<String, Integer> brandIdToSeriesNumber = new HashMap<>();

    public void updateDescription(Brand brand, Series series) {
        Optional<Integer> sourceSeriesOptional =
                getDescriptionSourceSeriesNumber(brand.getCanonicalUri());

        if(brandHasOwnDescription(brand, sourceSeriesOptional)) {
            return;
        }

        if(!sourceSeriesOptional.isPresent()) {
            updateDescriptionFromSeries(brand, series.getSeriesNumber(), series.getDescription());
            return;
        }

        Integer sourceSeries = sourceSeriesOptional.get();
        if(sourceSeries > series.getSeriesNumber()) {
            updateDescriptionFromSeries(brand, series.getSeriesNumber(), series.getDescription());
        }
    }

    private boolean brandHasOwnDescription(Brand brand, Optional<Integer> sourceSeriesOptional) {
        return brand.getDescription() != null && !sourceSeriesOptional.isPresent();
    }

    private void updateDescriptionFromSeries(Brand brand, Integer seriesNumber,
            String seriesDescription) {
        brand.setDescription(seriesDescription);
        setDescriptionSourceSeriesNumber(
                brand.getCanonicalUri(), seriesNumber
        );
    }

    private Optional<Integer> getDescriptionSourceSeriesNumber(String canonicalUri) {
        return Optional.fromNullable(brandIdToSeriesNumber.get(canonicalUri));
    }

    private void setDescriptionSourceSeriesNumber(String canonicalUri, Integer seriesNumber) {
        brandIdToSeriesNumber.put(canonicalUri, seriesNumber);
    }
}
