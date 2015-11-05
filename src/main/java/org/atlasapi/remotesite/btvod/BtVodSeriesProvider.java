package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class BtVodSeriesProvider {

    /**
     * guid -> series
     */
    private final Map<String, Series> explicitSeries;

    /**
     * sythesized series uri -> series
     */
    private final Map<String, Series> synthesizedSeries;

    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final CertificateUpdater certificateUpdater;

    public BtVodSeriesProvider(
            Map<String, Series> explicitSeries,
            Map<String, Series> synthesizedSeries,
            BtVodSeriesUriExtractor seriesUriExtractor,
            CertificateUpdater certificateUpdater) {
        this.explicitSeries = ImmutableMap.copyOf(explicitSeries);
        this.synthesizedSeries = ImmutableMap.copyOf(synthesizedSeries);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.certificateUpdater = checkNotNull(certificateUpdater);
    }

    public Optional<Series> seriesFor(BtVodEntry row) {
        if (row.getParentGuid() != null && explicitSeries.containsKey(row.getParentGuid())) {
            return Optional.of(explicitSeries.get(row.getParentGuid()));
        }

        Optional<String> seriesUri = seriesUriExtractor.seriesUriFor(row);
        if (seriesUri.isPresent() && synthesizedSeries.containsKey(seriesUri.get())) {
            return
                    Optional.of(
                            synthesizedSeries.get(seriesUri.get())
                    );
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
}
