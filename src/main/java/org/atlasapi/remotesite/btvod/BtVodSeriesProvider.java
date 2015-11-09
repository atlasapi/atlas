package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

public class BtVodSeriesProvider {

    /**
     * guid -> series
     */
    private final Map<String, Series> explicitSeries;

    /**
     * parentRef, series number -> series
     */
    private final Table<ParentRef, Integer, Series> explicitSeriesTable;

    /**
     * sythesized series uri -> series
     */
    private final Map<String, Series> synthesizedSeries;

    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final CertificateUpdater certificateUpdater;
    private final BtVodBrandProvider brandProvider;

    public BtVodSeriesProvider(
            Map<String, Series> explicitSeries,
            Map<String, Series> synthesizedSeries,
            BtVodSeriesUriExtractor seriesUriExtractor,
            CertificateUpdater certificateUpdater,
            BtVodBrandProvider brandProvider) {
        this.explicitSeries = ImmutableMap.copyOf(explicitSeries);
        this.explicitSeriesTable = getSeriesTable(explicitSeries);
        this.synthesizedSeries = ImmutableMap.copyOf(synthesizedSeries);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.certificateUpdater = checkNotNull(certificateUpdater);
        this.brandProvider = checkNotNull(brandProvider);
    }

    public Optional<Series> seriesFor(BtVodEntry row) {
        if (row.getParentGuid() != null && explicitSeries.containsKey(row.getParentGuid())) {
            return Optional.of(explicitSeries.get(row.getParentGuid()));
        }

        Optional<String> seriesUri = seriesUriExtractor.seriesUriFor(row);
        if (seriesUri.isPresent() && synthesizedSeries.containsKey(seriesUri.get())) {
            return Optional.of(synthesizedSeries.get(seriesUri.get()));
        }

        Optional<ParentRef> parentRef = brandProvider.brandRefFor(row);
        Optional<Integer> seriesNumber = seriesUriExtractor.extractSeriesNumber(row);
        if (parentRef.isPresent() && seriesNumber.isPresent()
                && explicitSeriesTable.contains(parentRef.get(), seriesNumber.get())) {
            return Optional.of(explicitSeriesTable.get(parentRef.get(), seriesNumber.get()));
        }

        return Optional.absent();
    }

    public void updateSeriesFromEpisode(BtVodEntry episodeRow, Episode episode) {
        Optional<Series> seriesOptional = seriesFor(episodeRow);
        if(!seriesOptional.isPresent()) {
            return;
        }
        Series series = seriesOptional.get();

        certificateUpdater.updateCertificates(series, episode);
    }

    private Table<ParentRef, Integer, Series> getSeriesTable(Map<String, Series> seriesMap) {
        ImmutableTable.Builder<ParentRef, Integer, Series> builder = ImmutableTable.builder();

        for (Series series : seriesMap.values()) {
            builder.put(series.getParent(), series.getSeriesNumber(), series);
        }

        return builder.build();
    }
}
