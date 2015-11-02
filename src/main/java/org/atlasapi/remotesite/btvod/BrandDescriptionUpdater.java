package org.atlasapi.remotesite.btvod;

import java.util.HashMap;
import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Optional;

public class BrandDescriptionUpdater {

    // Records the series number that is the source for the description/long description in a brand
    private Map<String, Integer> descriptionSourceEpisode = new HashMap<>();
    private Map<String, Integer> longDescriptionSourceEpisode = new HashMap<>();

    public void updateDescriptions(Brand brand, Series series) {
        updateDescription(brand, series);
        updateLongDescription(brand, series);
    }

    private void updateDescription(Brand brand, Series series) {
        Optional<Integer> sourceSeriesOptional =
                Optional.fromNullable(descriptionSourceEpisode.get(brand.getCanonicalUri()));

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
        if(seriesDescription == null) {
            return;
        }

        brand.setDescription(seriesDescription);
        descriptionSourceEpisode.put(brand.getCanonicalUri(), seriesNumber);
    }

    public void updateLongDescription(Brand brand, Series series) {
        Optional<Integer> sourceSeriesOptional =
                Optional.fromNullable(longDescriptionSourceEpisode.get(brand.getCanonicalUri()));

        if(brandHasOwnLongDescription(brand, sourceSeriesOptional)) {
            return;
        }

        if(!sourceSeriesOptional.isPresent()) {
            updateLongDescriptionFromSeries(
                    brand, series.getSeriesNumber(), series.getLongDescription()
            );
            return;
        }

        Integer sourceSeries = sourceSeriesOptional.get();
        if(sourceSeries > series.getSeriesNumber()) {
            updateLongDescriptionFromSeries(
                    brand, series.getSeriesNumber(), series.getLongDescription()
            );
        }
    }

    private boolean brandHasOwnLongDescription(Brand brand, Optional<Integer> sourceSeriesOptional) {
        return brand.getLongDescription() != null && !sourceSeriesOptional.isPresent();
    }

    private void updateLongDescriptionFromSeries(Brand brand, Integer seriesNumber,
            String seriesLongDescription) {
        if(seriesLongDescription == null) {
            return;
        }

        brand.setLongDescription(seriesLongDescription);
        longDescriptionSourceEpisode.put(brand.getCanonicalUri(), seriesNumber);
    }
}
