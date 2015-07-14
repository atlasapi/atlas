package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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

    public BtVodSeriesProvider(
            Map<String, Series> explicitSeries,
            Map<String, Series> synthesizedSeries,
            BtVodSeriesUriExtractor seriesUriExtractor
    ) {
        this.explicitSeries = ImmutableMap.copyOf(explicitSeries);
        this.synthesizedSeries = ImmutableMap.copyOf(synthesizedSeries);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
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
}
